-module(rabbitmq_prelaunch_dist).

-export([setup/1]).

setup(#{nodename := Node, nodename_type := NameType} = Context) ->
    rabbit_log_prelaunch:debug(""),
    rabbit_log_prelaunch:debug("== Erlang distribution =="),
    rabbit_log_prelaunch:debug("Rqeuested node name: ~s (type: ~s)",
                               [Node, NameType]),
    case node() of
        nonode@nohost ->
            ok = rabbit_nodes_common:ensure_epmd(),
            ok = dist_port_range_check(Context),
            ok = dist_port_use_check(Context),
            ok = duplicate_node_check(Context),

            ok = do_setup(Context);
        Node ->
            rabbit_log_prelaunch:debug(
              "Erlang distribution already running", []),
            ok;
        Unexpected ->
            rabbit_log_prelaunch:error(
              "Erlang distribution running with another node name (~s) "
              "than the configured one (~s)",
              [Unexpected, Node]),
            throw({error, erlang_dist_running_with_unexpected_nodename})
    end,
    ok.

do_setup(#{nodename := Node, nodename_type := NameType}) ->
    rabbit_log_prelaunch:debug("Starting Erlang distribution", []),
    case application:get_env(kernel, net_ticktime) of
        {ok, Ticktime} when is_integer(Ticktime) andalso Ticktime >= 1 ->
            %% The value passed to net_kernel:start/1 is the
            %% "minimum transition traffic interval" as defined in
            %% net_kernel:set_net_ticktime/1.
            MTTI = Ticktime * 1000 div 4,
            {ok, _} = net_kernel:start([Node, NameType, MTTI]);
        _ ->
            {ok, _} = net_kernel:start([Node, NameType])
    end,
    ok.

%% Check whether a node with the same name is already running
duplicate_node_check(#{split_nodename := {NodeName, NodeHost}}) ->
    rabbit_log_prelaunch:debug(
      "Checking if node name ~s is already used", [NodeName]),
    PrelaunchName = rabbit_nodes:make(
                      {NodeName ++ "_prelaunch_" ++ os:getpid(),
                       "localhost"}),
    {ok, _} = net_kernel:start([PrelaunchName, shortnames]),
    case rabbit_nodes:names(NodeHost) of
        {ok, NamePorts}  ->
            case proplists:is_defined(NodeName, NamePorts) of
                true ->
                    rabbit_log_prelaunch:error(
                      "Node with name ~p already running on ~p",
                      [NodeName, NodeHost]),
                    throw({error, duplicate_node_name});
                false ->
                    net_kernel:stop(),
                    ok
            end;
        {error, EpmdReason} ->
            rabbit_log_prelaunch:error(
              "epmd error for host ~s: ~s",
              [NodeHost, rabbit_misc:format_inet_error(EpmdReason)]),
            throw({error, epmd_error})
    end.

dist_port_range_check(#{erlang_dist_tcp_port := DistTcpPort}) ->
    rabbit_log_prelaunch:debug(
      "Checking if TCP port ~b is valid", [DistTcpPort]),
    case DistTcpPort of
        _ when DistTcpPort < 1 orelse DistTcpPort > 65535 ->
            rabbit_log_prelaunch:error(
              "Invalid Erlang distribution TCP port: ~b", [DistTcpPort]),
            throw({error, invalid_dist_port_range});
        _ ->
            ok
    end.

dist_port_use_check(#{split_nodename := {_, NodeHost},
                      erlang_dist_tcp_port := DistTcpPort}) ->
    rabbit_log_prelaunch:debug(
      "Checking if TCP port ~b is available", [DistTcpPort]),
    dist_port_use_check_ipv4(NodeHost, DistTcpPort).

dist_port_use_check_ipv4(NodeHost, Port) ->
    case gen_tcp:listen(Port, [inet, {reuseaddr, true}]) of
        {ok, Sock} -> gen_tcp:close(Sock);
        {error, einval} -> dist_port_use_check_ipv6(NodeHost, Port);
        {error, _} -> dist_port_use_check_fail(Port, NodeHost)
    end.

dist_port_use_check_ipv6(NodeHost, Port) ->
    case gen_tcp:listen(Port, [inet6, {reuseaddr, true}]) of
        {ok, Sock} -> gen_tcp:close(Sock);
        {error, _} -> dist_port_use_check_fail(Port, NodeHost)
    end.

-spec dist_port_use_check_fail(non_neg_integer(), string()) ->
                                         no_return().

dist_port_use_check_fail(Port, Host) ->
    {ok, Names} = rabbit_nodes:names(Host),
    case [N || {N, P} <- Names, P =:= Port] of
        [] ->
            rabbit_log_prelaunch:error(
              "Distribution port ~b in use on ~s "
              "(by non-Erlang process?)~n", [Port, Host]);
        [Name] ->
            rabbit_log_prelaunch:error(
              "Distribution port ~b in use by ~s@~s~n", [Port, Name, Host])
    end,
    throw({error, dist_port_already_used}).
