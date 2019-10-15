-module(rabbitmq_prelaunch_helpers).

-export([get_env/1, set_env/2]).

get_env(Key) ->
    application:get_env(rabbitmq_prelaunch, Key).

set_env(Key, Value) ->
    ok = application:set_env(
           rabbitmq_prelaunch, Key, Value, [{persistent, true}]).
