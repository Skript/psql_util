-module(psql_util_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link(Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Args]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([{SizeArgs, WorkerArgs}]) ->
    DB = {db, {db, start_link, [{SizeArgs, WorkerArgs}]}, permanent, infinity, supervisor, [db]},
    Transactor = {db_transactor, {db_transactor, start_link, []}, permanent, 2000, worker, [db_transactor]},

    {ok, { {one_for_one, 5, 10}, [DB, Transactor]} }.

