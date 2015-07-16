-module(psql_util_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    SizeArgs = application:get_env(psql_util, pool, [{size, 10}, {max_overflow, 20}]),
    WorkerArgs = application:get_env(psql_util, db, []),
    psql_util_sup:start_link({SizeArgs, WorkerArgs}).

stop(_State) ->
    ok.
