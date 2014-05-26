-module(fdb_raw).

-export([ api_version/1
        , clear/2
        , clear_range/3
        , get/2
        , get_range/2
        , get_range/3
        , init/0
        , init/1
        , init_and_open/0
        , init_and_open/1
        , init_and_open_try_5_times/1
        , maybe_do/1
        , next/2
        , open/0
        , previous/2
        , set/3
        , transact/2
        ]).

-define (FUTURE_TIMEOUT, 5000).
-include("../include/fdb.hrl").
-include("fdb_int.hrl").

-type fdb_get_option() :: {timeout, integer()}.

%% @doc Retrieve the key/value pair immediately succeeding the given
%% key.
-spec next(fdb_handle(), fdb_key()) -> ({ok,{fdb_key(),term()}} | not_found | {error,nif_not_loaded}).
%% @end
next(Handle, Key) ->
  Result = get_range(Handle, #select{ gt = Key, limit = 1 }),
  case Result of
    [] -> not_found;
    [{K, V}] -> {ok, {K, V}}
  end.

%% @doc Retrieve the key/value pair immediately preceeding the given
%% key.
-spec previous(fdb_handle(), fdb_key()) -> ({ok,{fdb_key(),term()}} | not_found | {error,nif_not_loaded}).
%% @end
previous(Handle, Key) ->
  Result = get_range(Handle, #select{ lt = Key, limit = 1, is_reverse = true }),
  case Result of
    [] -> not_found;
    [{K, V}] -> {ok, {K, V}}
  end.

maybe_do(Fs) ->
  Wrapped = lists:map(fun wrap_fdb_result_fun/1, Fs),
  maybe:do(Wrapped).

wrap_fdb_result_fun(F) ->
  case erlang:fun_info(F, arity) of
   {arity, 0} -> fun( ) -> handle_fdb_result(F()) end;
   {arity, 1} -> fun(X) -> handle_fdb_result(F(X)) end;
   _ -> throw({error, unsupported_arity })
  end.

%% @doc Loads the native FoundationDB library file from a certain location
-spec init(SoFile::list())-> ok | {error, term()}.
%% @end
init(SoFile) -> fdb_nif:init(SoFile).

%% @doc Loads the native FoundationDB library file from  `priv/fdb_nif.so`
-spec init()-> ok | {error, term()}.
%% @end
init() ->
  PrivDir = case code:priv_dir(?MODULE) of
              {error, bad_name} ->
                EbinDir = filename:dirname(code:which(?MODULE)),
                AppPath = filename:dirname(EbinDir),
                filename:join(AppPath, "priv");
              Path ->
                Path
            end,
  init(filename:join(PrivDir,"fdb_nif")).

%% @doc Specify the API version we are using
%%
%% This function must be called after the init, and before any other function in
%% this library is called.
-spec api_version(fdb_version()) -> fdb_cmd_result().
%% @end
api_version(Version) ->
  handle_fdb_result(fdb_nif:fdb_select_api_version(Version)).

%% @doc  Opens the given database
%%
%% (or the default database of the cluster indicated by the fdb.cluster file in a
%% platform-specific location, if no cluster_file or database_name is provided).
%% Initializes the FDB interface as required.
-spec open() -> fdb_database().
%% @end
open() ->
  maybe_do(
    [ fun () -> fdb_nif:fdb_setup_network() end
    , fun () -> fdb_nif:fdb_run_network() end
    , fun () -> fdb_nif:fdb_create_cluster() end
    , fun (ClF) -> future_get(ClF, cluster) end
    , fun (ClHandle) -> fdb_nif:fdb_cluster_create_database(ClHandle) end
    , fun (DatabaseF) -> future_get(DatabaseF, database) end
    , fun (DbHandle) ->
        %% check, whether database is actually available
        try
          get({db, DbHandle}, <<>>, [{timeout,1000}]),
          {ok, {db, DbHandle}}
        catch
          throw:fdb_timeout -> {error, fdb_timeout}
        end
      end ]).

%% @doc Initializes the driver and returns a database handle
-spec init_and_open() -> fdb_database().
%% end
init_and_open() ->
  init(),
  api_version(100),
  maybe_do([
    fun() -> open() end
  ]).

%% @doc Initializes the driver and returns a database handle
-spec init_and_open(SoFile::list()) -> fdb_database().
%% end
init_and_open(SoFile) ->
  init(SoFile),
  api_version(100),
  maybe_do([
    fun() -> open() end
  ]).

init_and_open_try_5_times(SoFile) ->
  init_and_open_try_5_times(SoFile, 5).

init_and_open_try_5_times(SoFile, N) ->
  case init_and_open(SoFile) of
    {ok, DB} -> {ok, DB};
    Error    -> case N of
      0 -> Error;
      _ ->
        timer:sleep(100),
        init_and_open_try_5_times(SoFile, N-1)
    end
  end.

get(DB, Key) -> get(DB, Key, []).

%% @doc Gets a value using a key
-spec get(fdb_handle(), fdb_key(), [fdb_get_option()]) -> {ok, term()} | not_found.
%% @end
get(DB={db, _Database}, Key, Options) ->
  transact(DB, fun(Tx) -> get(Tx, Key, Options) end);
get({tx, Tx}, Key, Options) ->
  Timeout = lkup(timeout, Options, ?FUTURE_TIMEOUT),
  maybe_do(
    [ fun()-> fdb_nif:fdb_transaction_get(Tx, Key) end
    , fun(GetF) -> future_get(GetF, value, Timeout) end
    , fun(Result) ->
        case Result of
          %% the result from future_get_value nif is either 'not_found' or a binary
          not_found -> not_found;
          _         -> {ok, Result}
        end
      end]);
get(_,_,_) ->
  ?THROW_FDB_ERROR(invalid_fdb_handle).

%% @doc Gets a range of key-value tuples where `begin <= X < end`
-spec get_range(fdb_handle(), fdb_key(),fdb_key()) -> ([term()]|{error,nif_not_loaded}).
%% @end
get_range(Handle, Begin, End) ->
  get_range(Handle, #select{gte = Begin, lt = End}).

%% @doc Gets a range of key-value tuples where `begin <= X < end`
-spec get_range(fdb_handle(), #select{}) -> ([term()]|{error,nif_not_loaded}).
%% @end
get_range(DB={db,_}, Select = #select{}) ->
  transact(DB, fun(Tx) -> get_range(Tx, Select) end);
get_range(Tx={tx, _}, Select = #select{}) ->
  Iterator = bind(Tx, Select),
  Next = next_iterator(Iterator),
  Next#iterator.data;
get_range(_,_) ->
  ?THROW_FDB_ERROR(invalid_fdb_handle).

%% @doc Binds a range of data to an iterator; use `fdb:next` to iterate it
-spec bind(fdb_handle(), #select{}) -> #iterator{}.
%% @end
bind(DB={db, _}, Select = #select{}) ->
  transact(DB, fun(Tx) -> bind(Tx, Select) end);
bind({tx, Transaction}, Select = #select{}) ->
  #iterator{tx = Transaction, select = Select, iteration = Select#select.iteration}.

%% @doc Get data of an iterator; returns the iterator or `done` when finished
-spec next_iterator(#iterator{}) -> (#iterator{}).
%% @end
next_iterator(Iterator = #iterator{tx = Transaction, iteration = Iteration, select = Select}) ->
  {FstKey, FstIsEq, FstOfs} = fst_gt(Select#select.gt, Select#select.gte),
  {LstKey, LstIsEq, LstOfs} = lst_lt(Select#select.lt, Select#select.lte),
  maybe_do([
   fun() -> fdb_nif:fdb_transaction_get_range(Transaction,
      FstKey, FstIsEq, Select#select.offset_begin + FstOfs,
      <<LstKey/binary>>, LstIsEq, Select#select.offset_end + LstOfs,
      Select#select.limit,
      Select#select.target_bytes,
      Select#select.streaming_mode,
      Iteration,
      Select#select.is_snapshot,
      Select#select.is_reverse) end,
   fun(F) -> {fdb_nif:fdb_future_is_ready(F),F} end,
   fun(Ready) -> wait_non_blocking(Ready) end,
   fun(F) -> future_get(F, keyvalue_array) end,
   fun(EncodedData) ->
     Iterator#iterator{
        data = EncodedData,
        iteration = Iteration + 1,
        out_more = false}
    end]).

%% These key selectors are used to find the borders of the range Low, High
%% Than all keys Low =< Key < High are returned

fst_gt(nil, nil)   -> { <<0>>, false, 1}; %% FDB_KEYSEL_FIRST_GREATER_OR_EQUAL
fst_gt(nil, Value) -> { Value, false, 1}; %% FDB_KEYSEL_FIRST_GREATER_OR_EQUAL
fst_gt(Value, nil) -> { Value, true , 1}. %% FDB_KEYSEL_FIRST_GREATER_THAN

lst_lt(nil, nil)   -> { <<255>>, false, 1}; %% FDB_KEYSEL_FIRST_GREATER_OR_EQUAL
                                            %% keys starting with <<255>> shall not be returned
lst_lt(nil, Value) -> { Value  , true , 1}; %% FDB_KEYSEL_FIRST_GREATER_THAN
lst_lt(Value, nil) -> { Value  , false, 1}. %% FDB_KEYSEL_FIRST_GREATER_OR_EQUAL

%% @doc sets a key and value
%% Existing values will be overwritten
-spec set(fdb_handle(), binary(), binary()) -> fdb_cmd_result().
%% @end
set({db, Database}, Key, Value) ->
  transact({db, Database}, fun (Tx)-> set(Tx, Key, Value) end);
set({tx, Tx}, Key, Value) ->
  ErrCode = fdb_nif:fdb_transaction_set(Tx, Key, Value),
  handle_fdb_result(ErrCode);
set(_,_,_) ->
  ?THROW_FDB_ERROR(invalid_fdb_handle).

%% @doc Clears a key and it's value
-spec clear(fdb_handle(), binary()) -> fdb_cmd_result().
%% @end
clear({db, Database}, Key) ->
  transact({db, Database}, fun (Tx)-> clear(Tx, Key) end);
clear({tx, Tx}, Key) ->
  ErrCode = fdb_nif:fdb_transaction_clear(Tx, Key),
  handle_fdb_result(ErrCode);
clear(_,_) ->
  ?THROW_FDB_ERROR(invalid_fdb_handle).

%% @doc Clears all keys where `begin <= X < end`
-spec clear_range(fdb_handle(), binary(), binary()) -> fdb_cmd_result().
%% @end
clear_range({db, Database}, Begin, End) ->
  transact({db, Database}, fun (Tx)-> clear_range(Tx, Begin, End) end);
clear_range({tx, Tx}, Begin, End) ->
  ErrCode = fdb_nif:fdb_transaction_clear_range(Tx, Begin, End),
  handle_fdb_result(ErrCode);
clear_range(_,_,_) ->
  ?THROW_FDB_ERROR(invalid_fdb_handle).

-spec transact(fdb_database(), fun((fdb_transaction())->term())) -> term().
transact({db, DbHandle}, DoStuff) ->
  CommitResult = attempt_transaction(DbHandle, DoStuff),
  handle_transaction_attempt(CommitResult);
transact(_,_) ->
  ?THROW_FDB_ERROR(invalid_fdb_handle).

attempt_transaction(DbHandle, DoStuff) ->
  ApplySelf = fun() -> attempt_transaction(DbHandle, DoStuff) end,
  maybe_do([
  fun() -> fdb_nif:fdb_database_create_transaction(DbHandle) end,
  fun(Tx) ->
      Result = DoStuff({tx, Tx}),
      CommitF = fdb_nif:fdb_transaction_commit(Tx),
      {future(CommitF), Tx, Result, ApplySelf}
     end
  ]).

handle_transaction_attempt({ok, _Tx, Result, _ApplySelf}) -> Result;
handle_transaction_attempt({{error, Err}, Tx, _Result, ApplySelf}) ->
  OnErrorF = fdb_nif:fdb_transaction_on_error(Tx, Err),
  maybe_do([
    fun () -> future(OnErrorF) end,
    fun () -> ApplySelf() end
  ]).

handle_fdb_result({0, RetVal}) -> {ok, RetVal};
handle_fdb_result({error, 2009}) -> ok;
handle_fdb_result({error, network_already_running}) -> ok;
handle_fdb_result({Err = {error, _}, _F}) -> Err;
handle_fdb_result(Other) -> Other.

future(F) -> future_get(F, none).

future_get(F, FQuery) -> future_get(F, FQuery, ?FUTURE_TIMEOUT).

future_get(F, FQuery, Timeout) ->
  maybe_do([
    fun() -> {fdb_nif:fdb_future_is_ready(F), F} end,
    fun(Ready) -> wait_non_blocking(Ready, Timeout) end,
    fun() -> fdb_nif:fdb_future_get_error(F) end,
    fun() -> get_future_property(F, FQuery) end
  ]).

get_future_property(_F,none) ->
  ok;
get_future_property(F,FQuery) ->
  FullQuery = list_to_atom("fdb_future_get_" ++ atom_to_list(FQuery)),
  apply(fdb_nif, FullQuery, [F]).

wait_non_blocking(Ready) -> wait_non_blocking(Ready, ?FUTURE_TIMEOUT).

wait_non_blocking({false,F}, Timeout) ->
  Ref = make_ref(),
  maybe_do([
  fun ()-> fdb_nif:send_on_complete(F,self(),Ref),
    receive
      Ref -> {ok, F}
      after Timeout -> throw(fdb_timeout)
    end
  end]);
wait_non_blocking({true,F}, _Timeout) ->
  {ok, F}.

lkup(K, KVs, Default) ->
  case lists:keysearch(K, 1, KVs) of
    {value, {K, V}} -> V;
    false           -> Default
  end.
