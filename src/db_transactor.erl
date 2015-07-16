-module(db_transactor).

-define(TIMEOUT, 2000).
-define(POOL_NAME, postgres).
-define(ETS_TAB, workers).

-export([transaction/1, transaction/2]).
-export ([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

transaction(Fun) ->
  transaction(Fun, ?TIMEOUT).

transaction(Fun, Timeout) ->
  Pid = self(),
  case in_transaction(Pid) of
    true ->
      Worker = get_worker(Pid),
      case erlang:fun_info(Fun, arity) of
          {arity, 0} -> Fun();
          {arity, 1} -> Fun(Worker)
      end;
    false ->
      Worker = begin_transaction(Pid, Timeout),
      try
        Res = case erlang:fun_info(Fun, arity) of
          {arity, 0} -> Fun();
          {arity, 1} -> Fun(Worker)
        end,
        commit_transaction(Worker, Pid),
        Res
      catch _:Reason ->
        lager:error("DB ERROR: ~p Backtrace ~p", [Reason, erlang:get_stacktrace()]),
        case Reason of
          {connection_hang, _} -> ok;
          _ -> rollback_transaction(Worker, Pid)
        end,
        error(Reason)
      end
  end.

-spec in_transaction(pid()) -> boolean().
in_transaction(Pid) ->
  case get_worker(Pid) of
    undefined -> false;
    Worker when is_pid(Worker) -> true
  end.

-spec get_worker(pid()) -> undefined | pid().
get_worker(Pid) ->
  gen_server:call(?MODULE, {get, Pid}).

-spec begin_transaction(pid(), integer()) -> pid().
begin_transaction(Pid, Timeout) ->
  Worker = poolboy:checkout(?POOL_NAME, true, 1000),
  db_worker:squery(Worker, 'begin', Timeout),
  gen_server:call(?MODULE, {'begin', Pid, Worker, Timeout}).

-spec commit_transaction(pid(), pid()) -> ok.
commit_transaction(Worker, Pid) ->
  end_transaction(Worker, Pid, 'commit'),
  ok.

-spec rollback_transaction(pid(), pid()) -> ok.
rollback_transaction(Worker, Pid) ->
  end_transaction(Worker, Pid, 'roll'),
  ok.

-spec end_transaction(pid(), pid(), 'roll' | 'commit') -> ok.
end_transaction(Worker, Pid, Query) ->
  db_worker:squery(Worker, Query, ?TIMEOUT),
  poolboy:checkin(?POOL_NAME, Worker),
  gen_server:call(?MODULE, {'end', Pid, Worker}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  check_ets(),
  {ok, ok}.

handle_call({get, Pid}, _From, State) ->
  check_ets(),
  Result = case ets:lookup(?ETS_TAB, Pid) of
    [] -> undefined;
    [{Pid, Worker}] ->
      case erlang:process_info(Worker) of
        undefined ->
          ets:delete(?ETS_TAB, Pid),
          undefined;
        _ ->
          Worker
      end
  end,
  {reply, Result, State};
handle_call({'begin', Pid, Worker, Timeout}, _From, State) ->
  check_ets(),
  true = ets:insert(?ETS_TAB, {Pid, Worker}),
  {reply, Worker, State};
handle_call({'end', Pid, Worker}, _From, State) ->
  check_ets(),
  true = ets:delete(?ETS_TAB, Pid),
  {reply, ok, State}.

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_Message, State) -> {noreply, State}.

terminate(timeout, _State) ->
  ok;
terminate(_,_) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


check_ets() ->
  case ets:info(?ETS_TAB) of
    undefined -> ets:new(?ETS_TAB, [named_table]);
    _ -> ok
  end.
