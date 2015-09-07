-module (db).
-include("types.hrl").

-behaviour (supervisor).

-export([start_link/1]).
-export([init/1]).
-export([transaction/1]).
-export([
  fetch_column_by/2,
  fetch_column_by/3,
  fetch_column_by/4,
  fetch_multiple_columns_by/2,
  fetch_multiple_columns_by/3,
  fetch_multiple_columns_by/4,
  find_all_by/2,
  find_one_by/2,
  fetch_raw/1,
  fetch_raw/2]).
-export([insert/2, insert/3, insert/4]).
-export([update/2, update/3]).
-export([delete/2, delete/3]).
-export([equery/3]).


-type worker() :: pid().

-type table() :: items | unique_items | lists
                 | users | devices | user_groups | user_usergroup | send_plans
                 | user_subscriptions | subscriptions | restrictions | purchases
                 | {table(), join, table(), on, {column_spec(), comp(), column_spec()}}.
-type column_spec() :: {table(), term()}.

-type filter() :: [filter_cond()].
-type filter_cond() :: {term(), '= any', atom()} | {atom(), term()} | {atom(), in, list()} | {atom(), comp(), term()}.
-type comp() :: '>' | '<' | '=' | '<>' | 'in' | '= any'.

-type update_filter() :: table() | {table(), filter_cond()} | {table(), filter()}.

-type returns() :: [] | [atom()] | '*'.

-type values() :: [{atom(), term()}].

-export_type([table/0]).

-export_type([column_spec/0, filter/0, returns/0, update_filter/0, values/0, worker/0]).

-define(POOL_NAME, postgres).
-define(TIMEOUT, 5000).

start_link(Args) ->
  supervisor:start_link({local, db}, ?MODULE, Args).

init(Args) ->
  {SizeArgs, WorkerArgs} = Args,
  PoolArgs = [{name, {local, ?POOL_NAME}},
    {worker_module, db_worker}]
    ++ SizeArgs,
  PoolSpec = poolboy:child_spec(?POOL_NAME, PoolArgs, WorkerArgs),
  {ok, {{one_for_one, 1000, 10}, [PoolSpec]}}.

-spec transaction(fun((worker()) -> Result)) -> Result when Result::any().
transaction(Fun) ->
  db_transactor:transaction(Fun, ?TIMEOUT).

-spec fetch_column_by(column_spec(), filter()) -> {ok, list()}.
fetch_column_by(ColumnSpec, Filter) ->
  fetch_column_by(ColumnSpec, Filter, []).

-spec fetch_column_by(column_spec(), filter(), plist()) -> {ok, list()}.
fetch_column_by(ColumnSpec, Filter, Extra) ->
  db_transactor:transaction(
    fun(Worker) ->
      fetch_column_by(Worker, ColumnSpec, Filter, Extra)
    end,
    ?TIMEOUT).

-spec fetch_multiple_columns_by(column_spec(), filter()) -> {ok, [plist()]}.
fetch_multiple_columns_by(ColumnSpec, Filter) ->
  fetch_multiple_columns_by(ColumnSpec, Filter, [], ?TIMEOUT).

-spec fetch_multiple_columns_by(column_spec(), filter(), plist()) -> {ok, list()}.
fetch_multiple_columns_by(ColumnSpec, Filter, Extra) ->
  fetch_multiple_columns_by(ColumnSpec, Filter, Extra, ?TIMEOUT).

-spec fetch_multiple_columns_by(column_spec(), filter(), plist(), integer()) -> {ok, list()}.
fetch_multiple_columns_by(ColumnSpec, Filter, Extra, Timeout) ->
  db_transactor:transaction(
    fun(Worker) ->
      fetch_multiple_columns_by(Worker, ColumnSpec, Filter, Extra, Timeout)
    end,
    Timeout).

-spec fetch_raw(binary()) -> {ok, plist()}.
fetch_raw(Query) ->
  fetch_raw(Query, ?TIMEOUT).

-spec fetch_raw(binary(), integer()) -> {ok, plist()}.
fetch_raw(Query, Timeout) ->
  db_transactor:transaction(
    fun(Worker) ->
      db_worker:fetch_raw(Worker, Query, Timeout)
    end,
    Timeout).

-spec find_all_by(table(), filter()) -> {ok, [plist()]}.
find_all_by(Table, Filter) ->
  db_transactor:transaction(
    fun(Worker) ->
      find_all_by(Worker, Table, Filter)
    end,
    ?TIMEOUT).

-spec find_one_by(table(), filter()) -> {ok, plist()} | {error, not_found}.
find_one_by(Table, Filter) ->
  db_transactor:transaction(
    fun(Worker) ->
      find_one_by(Worker, Table, Filter)
    end,
    ?TIMEOUT).

-spec update(update_filter(), values()) -> ok.
update(Filter, Values) ->
  db_transactor:transaction(
    fun(Worker) ->
      update(Worker, Filter, Values)
    end,
    ?TIMEOUT).

-spec insert(table(), values(), returns()) -> ok | {ok, plist()}.
insert(Table, Values, Returns) ->
  db_transactor:transaction(
    fun(Worker) ->
      insert(Worker, Table, Values, Returns)
    end,
    ?TIMEOUT).

-spec insert(table(), values()) -> ok.
insert(Table, Values) ->
  insert(Table, Values, []).


-spec delete(table(), filter()) -> ok.
delete(Table, Filter) ->
  db_transactor:transaction(
    fun(Worker) ->
      delete(Worker, Table, filter_deleted(Table, Filter))
    end,
    ?TIMEOUT).

-spec delete(worker(), table(), filter()) -> ok.
delete(Worker, Table, Filter) ->
  db_worker:delete(Worker, Table, filter_deleted(Table, Filter), ?TIMEOUT).

-spec fetch_column_by(worker(), column_spec(), filter(), plist()) -> {ok, list()}.
fetch_column_by(Worker, ColumnSpec, Filter, Extra) ->
  {Table, _} = ColumnSpec,
  FilterWithDeleted = filter_deleted(Table, Filter),
  db_worker:fetch_column_by(Worker, ColumnSpec, FilterWithDeleted, Extra, ?TIMEOUT).

-spec fetch_multiple_columns_by(worker(), column_spec(), filter(), plist(), integer()) -> {ok, list()}.
fetch_multiple_columns_by(Worker, ColumnSpec, Filter, Extra, Timeout) ->
  Table = case ColumnSpec of
    {T, _} -> T;
    {T, _, _} -> T
  end,
  FilterWithDeleted = filter_deleted(Table, Filter),
  db_worker:fetch_multiple_columns_by(Worker, ColumnSpec, FilterWithDeleted, Extra, Timeout).


-spec find_all_by(worker(), table(), filter()) -> {ok, [plist()]}.
find_all_by(Worker, Table, Filter) ->
  FilterWithDeleted = filter_deleted(Table, Filter),
  db_worker:find_all_by(Worker, Table, FilterWithDeleted, ?TIMEOUT).

-spec find_one_by(worker(), table(), filter()) -> {ok, plist()} | {error, not_found}.
find_one_by(Worker, Table, Filter) ->
  FilterWithDeleted = filter_deleted(Table, Filter),
  db_worker:find_one_by(Worker, Table, FilterWithDeleted, ?TIMEOUT).

-spec update(worker(), update_filter(), values()) -> ok.
update(Worker, Table, Values) when is_atom(Table) ->
  update(Worker, {Table, []}, Values);
update(Worker, {Table, Filter}, Values) when is_atom(Table), is_tuple(Filter)  ->
  update(Worker, {Table, [Filter]}, Values);
update(Worker, {Table, Filter}, Values) ->
  FilterWithDeleted = filter_deleted(Table, Filter),
  db_worker:update(Worker, {Table, FilterWithDeleted}, Values ++ timestamps(Table, update), ?TIMEOUT).

-spec insert(worker(), table(), values(), returns()) -> ok | {ok, plist()}.
insert(Worker, Table, Values, Returns) ->
  db_worker:insert(
    Worker,
    Table,
    Values ++ timestamps(Table, insert),
    Returns,
    ?TIMEOUT).

-type action() :: insert | update.
-spec timestamps(table(), action()) -> [{atom(), term()}].
timestamps(Table, Action) ->
  case {is_timestampable(Table), Action} of 
    {true, insert} ->
      [
        {create_timestamp, calendar:universal_time()},
        {update_timestamp, calendar:universal_time()}
      ];
    {true, update} ->
      [{update_timestamp, calendar:universal_time()}];
    {false, _} ->
      []
  end ++
  case {is_soft_deletable(Table), Action} of
    {true, insert} ->
      [{delete_timestamp, {{1970,1,1},{0,0,0}}}];
    _ -> []
  end.


is_timestampable(Table) when
  Table == devices;
  Table == items;
  Table == lists;
  Table == product_groups;
  Table == purchases; 
  Table == restrictions; 
  Table == send_plans;
  Table == subscribtions;
  Table == unique_items;
  Table == user_subscriptions; 
  Table == users 
  -> true;
is_timestampable(_) -> false.

is_soft_deletable(Table) when 
  Table == devices;
  Table == items;
  Table == lists;
  Table == product_groups;
  Table == send_plans;
  Table == subscribtions;
  Table == unique_items;
  Table == user_groups; 
  Table == users 
  -> true;
is_soft_deletable(_) -> false.

-spec filter_deleted(table(), filter()) -> filter().
filter_deleted({Table, join, OtherTable, on, _Cond}, Filter) ->
  filter_deleted(OtherTable, filter_deleted(Table, Filter));
filter_deleted(Table, Filter) ->
  case is_soft_deletable(Table) of
    true ->
      case lists2:keyfind(delete_timestamp, Filter) of
        undefined ->
          IdFilter = lists:any(
            fun({id,_}) -> true;
               ({{_, id}, _}) -> true;
               (_) -> false
            end,
            Filter),
          case IdFilter of
            true -> Filter;
            false -> [{{Table,delete_timestamp}, '=', {{1970,1,1},{0,0,0}}}] ++ Filter
          end;
        no_filter -> proplists:delete(delete_timestamp, Filter);
        _ -> Filter
      end;
    false -> Filter
  end.

equery(Query, Params, Timeout) ->
  db_transactor:transaction(
    fun(Worker) ->
      db_worker:equery(Worker, Query, Params, Timeout)
    end).
