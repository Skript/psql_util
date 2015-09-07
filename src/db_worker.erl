-module(db_worker).
-include("types.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-behaviour(poolboy_worker).
-behaviour(gen_server).
-define(CONNECTION_TIMEOUT, 5000).

-record(state, {conn}).

-export([start_link/1]).
-export([insert/5, update/4, delete/4]).
-export([fetch_column_by/5, fetch_multiple_columns_by/5, find_all_by/4, find_one_by/4, fetch_raw/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([equery/4]).
-export([squery/3]).

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

init(Args) ->
  Host = lists2:keyfind(host, Args),
  Port = case lists2:keyfind(port, Args) of
    undefined -> 5432;
    P when is_integer(P) -> P;
    [] -> 5432;
    P when is_list(P) -> list_to_integer(P);
    P when is_binary(P) -> binary_to_integer(P)
  end,
  User = lists2:keyfind(user, Args),
  Password = lists2:keyfind(password, Args),
  Database = lists2:keyfind(dbname, Args),
  Timeout = lists2:keyfind(timeout, Args, ?CONNECTION_TIMEOUT),
  case epgsql:connect(Host, User, Password, [{port, Port},{database, Database}, {timeout, Timeout}]) of
    {ok, Conn} -> {ok, #state{conn=Conn}};
    {error, Err} -> {stop, {connection_failed, Err}}
  end.

-spec insert(db:worker(), db:table(), db:values(), db:returns(), integer()) -> ok | {ok, plist()}.
insert(Worker, Table, Values, Returns, Timeout) ->
  {Arguments, Params} = prepare_params(Values),
  Query = case Returns of
    [] -> {insert, Table, Arguments};
    Returns -> {insert, Table, Arguments, {returning, Returns}}
  end,
  Result = equery(Worker, Query, Params, Timeout),
  parse_result(Result).

-spec update(db:worker(), db:update_filter(), db:values(), integer()) -> ok.
update(Worker, Table, Values, Timeout) when is_atom(Table) ->
  update(Worker, {Table, []}, Values, Timeout);
update(Worker, {Table, Filter}, Values, Timeout) when is_tuple(Filter) ->
  update(Worker, {Table, [Filter]}, Values, Timeout);
update(Worker, {Table, Filter}, Values, Timeout) ->
  {Arguments, Params} = prepare_params(Values),
  {Where, WhereParams} = prepare_filters(Filter, length(Params)),
  Query = case Filter of
    [] ->
      {update, Table, Arguments};
    _ ->
      {update, Table, Arguments, {where, Where}}
  end,
  Res = equery(Worker, Query, Params ++ WhereParams, Timeout),
  {ok, _} = Res,
  ok.

-spec delete(db:worker(), db:table(), db:filter(), integer()) -> ok.
delete(Worker, Table, Filter, Timeout) ->
  {Where, WhereParams} = prepare_filters(Filter),
  Query = {delete, {from, Table}, {where, Where}},
  {ok, _N} = equery(Worker, Query, WhereParams, Timeout),
  ok.

-spec fetch_column_by(db:worker(), db:column_spec(), db:filter(), plist(), integer()) -> {ok, list()}.
fetch_column_by(Worker, {Table, Column}, Filter, Extra, Timeout) when is_atom(Column); is_tuple(Column) ->
  {Where, WhereParams} = prepare_filters(Filter),
  ExtraQ = case Extra of [] -> []; _ -> [Extra] end,
  Query = list_to_tuple([select, Column, {from, Table}, {where, Where}] ++ ExtraQ),
  Result = equery(Worker, Query, WhereParams, Timeout),
  parse_scalar_result_list(Result).

-spec fetch_multiple_columns_by(db:worker(), db:column_spec(), db:filter(), plist(), integer()) -> {ok, plist()}.
fetch_multiple_columns_by(Worker, {Table, Columns}, Filter, Extra, Timeout) when is_list(Columns) ->
  fetch_multiple_columns_by(Worker, {Table, undefined, Columns}, Filter, Extra, Timeout);
fetch_multiple_columns_by(Worker, {Table, Modifier, Columns}, Filter, Extra, Timeout) when is_list(Columns) ->
  {Where, WhereParams} = prepare_filters(Filter),
  ExtraQ = case Extra of [] -> []; _ -> [Extra] end,
  Query = list_to_tuple([select, Modifier, Columns, {from, Table}, {where, Where}] ++ ExtraQ),
  Result = equery(Worker, Query, WhereParams, Timeout),
  parse_result_list(Result).

-spec find_all_by(db:worker(), db:table(), db:filter(), integer()) -> {ok, [plist()]}.
find_all_by(Worker, Table, Filter, Timeout) ->
  {Where, WhereParams} = prepare_filters(Filter),
  Query = {select, '*', {from, Table}, {where, Where}},
  Result = equery(Worker, Query, WhereParams, Timeout),
  parse_result_list(Result).

-spec find_one_by(db:worker(), db:table(), db:filter(), integer()) -> {error, not_found} | {ok, [plist()]}.
find_one_by(Worker, Table, Filter, Timeout) ->
  {Where, WhereParams} = prepare_filters(Filter),
  Query = {select, '*', {from, Table}, {where, Where}, {limit, 1}},
  Result = equery(Worker, Query, WhereParams, Timeout),
  parse_result(Result).

-spec fetch_raw(db:worker(), binary(), integer()) -> {ok, plist()}.
fetch_raw(Worker, Query, Timeout) ->
  Result = equery(Worker, Query, [], Timeout),
  parse_result_list(Result).

-type equery_result() :: {ok, integer()} |
                        {ok, [#column{}], [tuple()]} |
                        {ok, integer(), [#column{}], [tuple()]} |
                        {error, #error{}}.
-spec parse_result(equery_result()) -> ok | {error, not_found} | {ok, plist()}.
parse_result({error, Err}) ->
  error({db_error, Err});
parse_result({ok, _Count}) ->
  ok;
parse_result({ok, _Columns, []}) ->
  {error, not_found};
parse_result({ok, _Count, Columns, Rows}) ->
  parse_result({ok, Columns, Rows});
parse_result({ok, Columns, [FirstRow | _]}) ->
  {ok, [Result | _]} = parse_result_list({ok, Columns, [FirstRow]}),
  {ok, Result}.

-spec parse_result_list(equery_result()) -> ok | {ok, [plist()]}.
parse_result_list({error, Err}) ->
  lager:error("Error in parse ~p~n", [Err]),
  error(Err);
parse_result_list({ok, _Count}) ->
  ok;
parse_result_list({ok, _Columns, []}) ->
  {ok, []};
parse_result_list({ok, _Count, Columns, Rows}) ->
  parse_result_list({ok, Columns, Rows});
parse_result_list({ok, Columns, Rows}) ->
  ColumnNames = [ list_to_atom(binary_to_list(Name))
                  || #column{name = Name} <- Columns ],
  {ok, [ lists:zip(ColumnNames, tuple_to_list(Row)) || Row <- Rows ]}.

-spec parse_scalar_result_list(equery_result()) -> ok | {ok, [term()]}.
parse_scalar_result_list({ok, _Count}) ->
  ok;
parse_scalar_result_list({ok, _Columns, []}) ->
  {ok, []};
parse_scalar_result_list({ok, _Count, Columns, Rows}) ->
  parse_scalar_result_list({ok, Columns, Rows});
parse_scalar_result_list({ok, _Columns, Rows}) ->
  {ok, [ Value || {Value} <- Rows ]};
parse_scalar_result_list({error, Err}) ->
  lager:error("Error in parse ~p~n", [Err]),
  error(Err).


-spec prepare_params(db:values()) -> {plist(), list()}.
prepare_params(Values) ->
  prepare_params(Values, 0).
-spec prepare_params(db:values(), integer()) -> {plist(), list()}.
prepare_params(Values, StartNumber) ->
  {InParams, NotInParams} = lists:partition(
    fun({_, {call, _, _}}) -> false;
       ({Key, {Key, _, _}}) -> false;
       (_) -> true
    end,
    Values),
  {Keys, Parameters} = lists:unzip(InParams),
  EndNumber = StartNumber + length(InParams),
  ArgumentsNumbers = [ list_to_atom("$" ++ integer_to_list(Num))
                       || Num <- lists:seq(StartNumber + 1, EndNumber) ],
  Arguments = lists:zip(Keys, ArgumentsNumbers),
  {Arguments ++ NotInParams, Parameters}.


-spec prepare_filters(db:filter()) -> {tuple(), list()}.
prepare_filters(Filter) ->
  prepare_filters(Filter, 0).
-spec prepare_filters(db:filter(), integer()) -> {tuple(), list()}.
prepare_filters([], _) -> {{true, '=', true}, []};
prepare_filters(Filter, ParametersFrom) ->
  {InCond, NotInFilters} = unzip_in_cond(Filter),
  FilterWithEq = add_eq_comparison(NotInFilters),
  {_,_,Parameters} = lists:unzip3(FilterWithEq),
  {FilterWithNums, _} = lists:mapfoldl(
    fun({Key, Comp, _Val}, Acc) ->
      Num = Acc + 1,
      ArgNum = list_to_atom("$" ++ integer_to_list(Num)),
      {{Key, Comp, ArgNum}, Num}
    end,
    ParametersFrom,
    FilterWithEq),
  {{'and', FilterWithNums ++ InCond}, Parameters}.

-spec add_eq_comparison(db:filter()) -> db:filter().
add_eq_comparison(Filter) ->
  lists:map(
    fun({Key, Val}) -> {Key, '=', Val};
       (El) -> El
    end,
    Filter).
-spec unzip_in_cond(db:filter()) -> {db:filter(), db:filter()}.
unzip_in_cond(Filter) ->
  {InCond, OtherFilters} = lists:partition(
    fun
      ({Key, _Op, {Key, _, _}}) -> true;
      ({_Key, _Op, {call, _, _}}) -> true;
      ({_Key, in, _List}) -> true;
      ({_Key, 'not in', _List}) -> true;
      ({_Key, 'is', null}) -> true;
      ({_Key, 'is not', null}) -> true;
      ({_Val, '= any', _Field}) -> true;
      ({'or', _}) -> true;
      ({'and', _}) -> true;
      ({'not', 'exists', _List}) -> true;
      ({{_, delete_timestamp}, '=', _Val}) -> true;
      ({delete_timestamp, '=', _Val}) -> true;
      ({_Val, '= any', _Key}) -> true;
      (_) -> false
    end,
    Filter),
  {
    lists:map(
      fun
        ({_Key, 'not in', []}) -> {true, '=', true};
        ({_Key, in, []}) -> {true, '=', false};
        (Other) -> Other
      end,
      InCond),
    OtherFilters
  }.

equery(Worker, Query, Params, Timeout) when is_binary(Query) ->
  {Time, Res} = timer:tc(gen_server, call, [Worker, {equery, Query, Params, Timeout}, Timeout]),
  % lager:error("TIMING DB query ~tp~n took ~tp ms~n trace ~p~n", [Query, Time / 1000, catch error(trace)]),
  case Res of
    {error, Err} -> lager:error("DB ERROR: ~tp~n In Query ~tp~n Params ~p~n", [Err, Query, Params]);
    _ -> ok
  end,
  Res;
equery(Worker, Stmt, Params, Timeout) ->
  Query = sqerl:sql(Stmt, true),
  equery(Worker, Query, Params, Timeout).

squery(Worker, Sql, Timeout) ->
  Query = sqerl:sql(Sql, true),
  gen_server:call(Worker, {squery, Query, Timeout}, Timeout).

%% Gen server behaviour

handle_call({squery, Query, Timeout}, _From, #state{conn=Conn}=State) ->
  {ok, Tref} = timer:exit_after(Timeout - 200, connection_hang),
  Res = epgsql:squery(Conn, Query),
  timer:cancel(Tref),
  {reply, Res, State};
handle_call({equery, Query, Params, Timeout}, _From, #state{conn=Conn}=State) ->
  {ok, Tref} = timer:exit_after(Timeout - 1000, connection_hang),
  Res = epgsql:equery(Conn, Query, Params),
  timer:cancel(Tref),
  {reply, Res, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{conn=Conn}) ->
  ok = epgsql:close(Conn),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
