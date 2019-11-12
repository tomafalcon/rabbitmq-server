%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_stream_queue_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

% -import(quorum_queue_utils, [wait_for_messages_ready/3,
%                              wait_for_messages_pending_ack/3,
%                              wait_for_messages_total/3,
%                              wait_for_messages/2,
%                              dirty_query/3,
%                              ra_name/1]).

-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
      {group, single_node},
      {group, clustered}
    ].

groups() ->
    [
     {single_node, [], all_tests()},
     {clustered, [], [
                      {cluster_size_3, [], []
                       ++ all_tests()}
                     ]}
    ].

all_tests() ->
    [
     roundtrip
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config = rabbit_ct_helpers:merge_app_env(
               Config0, {rabbit, [{quorum_tick_interval, 1000}]}),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_clustered, true}]);
init_per_group(Group, Config) ->
    ClusterSize = case Group of
                      single_node -> 1;
                      cluster_size_2 -> 2;
                      cluster_size_3 -> 3;
                      cluster_size_5 -> 5
                  end,
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, ClusterSize},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    Config1b = rabbit_ct_helpers:set_config(Config1, [{net_ticktime, 10}]),
    Config2 = rabbit_ct_helpers:run_steps(Config1b,
                                          [fun merge_app_env/1 ] ++
                                          rabbit_ct_broker_helpers:setup_steps()),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config2, quorum_queue) of
        ok ->
            ok = rabbit_ct_broker_helpers:rpc(
                   Config2, 0, application, set_env,
                   [rabbit, channel_tick_interval, 100]),
            %% HACK: the larger cluster sizes benefit for a bit more time
            %% after clustering before running the tests.
            case Group of
                cluster_size_5 ->
                    timer:sleep(5000),
                    Config2;
                _ ->
                    Config2
            end;
        Skip ->
            end_per_group(Group, Config2),
            Skip
    end.

end_per_group(clustered, Config) ->
    Config;
end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    % rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

end_per_testcase(Testcase, Config) ->
    % catch delete_queues(),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

roundtrip(Config) ->
    % Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    [Server | _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, lists:last(Servers)),
    QName = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 declare(Ch, QName, [{<<"x-queue-type">>, longstr, <<"stream">>}])),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    CTag1 = <<"ctag1">>,
    subscribe(Ch2, CTag1, QName, 100, []),
    publish_confirm(Ch, QName, <<"msg1">>),
    receive
        {#'basic.deliver'{delivery_tag = DT1,
                          consumer_tag = CTag1,
                          redelivered  = false}, Msg} ->
            ct:pal("GOT ~w ~w", [DT1, Msg]),
            flush(100),
            ok
    after 2000 ->
              exit(basic_deliver_timeout_1)
    end,
    publish_confirm(Ch, QName, <<"msg2">>),
    receive
        {#'basic.deliver'{delivery_tag = DT2,
                          consumer_tag = CTag1,
                          redelivered  = false}, Msg2} ->
            ct:pal("GOT ~w ~w", [DT2, Msg2]),
            flush(100),
            ok
    after 2000 ->
              exit(basic_deliver_timeout_2)
    end,
    %% another consumer can read
    Ch3 = rabbit_ct_client_helpers:open_channel(Config, Server),
    CTag2 = <<"ctag2">>,
    subscribe(Ch3, CTag2, QName, 1, []),
    receive
        {#'basic.deliver'{delivery_tag = DTag3,
                          consumer_tag = CTag2,
                          redelivered  = false}, Msg3} ->
            ct:pal("~s GOT ~w", [CTag2, Msg3]),
            amqp_channel:cast(Ch3, #'basic.ack'{delivery_tag = DTag3,
                                                multiple = false}),
            ok
    after 2000 ->
              exit(basic_deliver_timeout_3)
    end,
    receive
        {#'basic.deliver'{delivery_tag = DTag4,
                          consumer_tag = CTag2,
                          redelivered  = false}, Msg4} ->
            ct:pal("~s GOT ~w", [CTag2, Msg4]),
            amqp_channel:cast(Ch3, #'basic.ack'{delivery_tag = DTag4,
                                                multiple = false}),
            ok
    after 2000 ->
              exit(basic_deliver_timeout_4)
    end,
    cancel(Ch3, CTag2),
    flush(100),
    ok.


%% HELPERS

publish_confirm(Ch, QName, Msg) ->
    publish(Ch, QName, Msg),
    amqp_channel:register_confirm_handler(Ch, self()),
    ct:pal("waiting for confirms from ~s", [QName]),
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 2500 ->
                   exit(confirm_timeout)
         end,
    ct:pal("CONFIRMED! ~s", [QName]),
    ok.

publish_many(Ch, Queue, Count) ->
    [publish(Ch, Queue, <<I:16/integer>>) || I <- lists:seq(1, Count)].

publish(Ch, Queue, Msg) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = Queue},
                           #amqp_msg{props   = #'P_basic'{delivery_mode = 2},
                                     payload = Msg}).
declare(Ch, Q) ->
    declare(Ch, Q, []).

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue  = Q,
                                           durable = true,
                                           auto_delete = false,
                                           arguments = Args}).

subscribe(Ch, CTag, Queue, Prefetch, Args) ->
    qos(Ch, Prefetch),
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = true,
                                                arguments = Args,
                                                consumer_tag = CTag},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
             ok
    end.

cancel(Ch, CTag) ->
    amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}).

qos(Ch, Prefetch) ->
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                    prefetch_count = Prefetch})).

flush(T) ->
    receive X ->
                ct:pal("flushed ~w", [X]),
                flush(T)
    after T ->
              ok
    end.
