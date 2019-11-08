-module(rabbit_stream_index).

-export([
         new/0,
         incr/2,
         incr/3,
         get/2,
         current/1
         ]).

-type log_idx() :: non_neg_integer().
% -type raft_idx() :: non_neg_integer().

%% holds static or rarely changing fields
-record(cfg, {}).

-record(segment, {to = 0:: log_idx(),
                  from = 0 :: log_idx(),
                  offset = 0:: non_neg_integer()}).


-record(?MODULE, {cfg :: #cfg{},
                  segs :: [#segment{}]}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

new() ->
    #?MODULE{segs = [#segment{}]}.

incr(RaftIdx,
     #?MODULE{segs = [#segment{to = To,
                               offset = Offset} = Seg | Rem] = All} = State) ->
    NextLogIdx = To + 1,
    case RaftIdx - NextLogIdx of
        Offset ->
            %% no offset change
            %% just update to
            #?MODULE{segs = [Seg#segment{to = NextLogIdx} | Rem]};
        NewOffset ->
            State#?MODULE{segs = [Seg#segment{to = NextLogIdx,
                                              from =  NextLogIdx,
                                              offset = NewOffset} | All]}
    end.

current(#?MODULE{segs = [#segment{to = To} | _]}) ->
    To.

get(LogIdx, #?MODULE{segs = Segs}) ->
    scan(LogIdx, Segs).

scan(Idx, [#segment{to = To, from = F, offset = Offs} | Rem]) ->
    case Idx =< To andalso Idx >= F of
        true ->
            %% return raft index
            Idx + Offs;
        false ->
            scan(Idx, Rem)
    end;
scan(_Idx, []) ->
    undefined.



incr(From, To, State) ->
    lists:foldl(fun (I, Acc) ->
                        incr(I, Acc)
                end, State, lists:seq(From, To)).



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

