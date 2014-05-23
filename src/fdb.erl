-module(fdb).

%% This is basically just tuple and term layer on top of fdb_raw
%% All keys are encoded/decoded using tuple:pack/unpack
%% All values are encoded/decoded using term_to_binary/binary_to_term

-export([init/0, init/1]).
-export([api_version/1, open/0]).
-export([get/2, get/3, get_range/2, get_range/3, set/3]).
-export([clear/2, clear_range/3]).
-export([transact/2]).
-export([init_and_open/0, init_and_open/1]).
-export([error_as_atom/1]).

-include("../include/fdb.hrl").

%% @doc Loads the native FoundationDB library file from a certain location
-spec init(SoFile::list())-> ok | {error, term()}.
%% @end
init(SoFile) -> fdb_raw:init(SoFile).

%% @doc Loads the native FoundationDB library file from  `priv/fdb_nif.so`
-spec init()-> ok | {error, term()}.
%% @end
init() -> fdb_raw:init().

%% @doc Specify the API version we are using
%%
%% This function must be called after the init, and before any other function in
%% this library is called.
-spec api_version(fdb_version()) -> fdb_cmd_result().
%% @end
api_version(Version) ->
  fdb_raw:api_version(Version).

%% @doc  Opens the given database 
%% 
%% (or the default database of the cluster indicated by the fdb.cluster file in a 
%% platform-specific location, if no cluster_file or database_name is provided).  
%% Initializes the FDB interface as required.
-spec open() -> fdb_database().
%% @end
open() -> fdb_raw:open().

%% @doc Initializes the driver and returns a database handle
-spec init_and_open() -> fdb_database().
%% end
init_and_open() -> fdb_raw:init_and_open().

%% @doc Initializes the driver and returns a database handle
-spec init_and_open(SoFile::list()) -> fdb_database().
%% end
init_and_open(SoFile) -> fdb_raw:init_and_open(SoFile).

%% @doc Gets a value using a key, falls back to a default value if not found
-spec get(fdb_handle(), fdb_key(), term()) -> term().
%% @end
get(Handle, Key, Default) ->
  case get(Handle, Key) of
    {ok, Value} -> Value;
    not_found   -> Default
  end.

%% @doc Gets a value using a key
-spec get(fdb_handle(), fdb_key()) -> {ok, term()} | not_found.
%% @end
get(Handle, Key) ->
  case fdb_raw:get(Handle, tuple:pack(Key)) of
    {ok, Value} -> {ok, binary_to_term(Value)};
    not_found   -> not_found
  end.

%% @doc Gets a range of key-value tuples where `begin <= X < end`
-spec get_range(fdb_handle(), fdb_key(),fdb_key()) -> ([term()]|{error,nif_not_loaded}).
%% @end
get_range(Handle, Begin, End) ->
  get_range(Handle, #select{gte = Begin, lt = End}).

%% @doc Gets a range of key-value tuples where `begin <= X < end`
-spec get_range(fdb_handle(), #select{}) -> ([term()]|{error,nif_not_loaded}).
%% @end
get_range(Handle, Select = #select{}) ->
  EncodedData = fdb_raw:get_range(Handle, encode(Select)),
  [ {tuple:unpack(K), binary_to_term(V)} || {K,V} <- EncodedData ].

%% @doc Map the internal Fdb error code to a readable atom name
-spec error_as_atom({error, integer()} | integer()) -> atom().
error_as_atom({error, E}) ->
  error_as_atom(E);
error_as_atom(E) when is_integer(E) ->
  fdb_nif:fdb_get_error(E).

%% @doc sets a key and value
%% Existing values will be overwritten
-spec set(fdb_handle(), fdb_key(), term()) -> fdb_cmd_result().
%% @end
set(Handle, Key, Value) ->
  fdb_raw:set(Handle, tuple:pack(Key), term_to_binary(Value)).

%% @doc Clears a key and it's value
-spec clear(fdb_handle(), fdb_key()) -> fdb_cmd_result().
%% @end
clear(Handle, Key) ->
  fdb_raw:clear(Handle, tuple:pack(Key)).

%% @doc Clears all keys where `begin <= X < end`
-spec clear_range(fdb_handle(), fdb_key(), fdb_key()) -> fdb_cmd_result().
%% @end
clear_range(Handle, Begin, End) ->
  fdb_raw:clear_range(Handle, tuple:pack(Begin), tuple:pack(End)).

-spec transact(fdb_database(), fun((fdb_transaction())->term())) -> term().
transact(DB = {db, _DbHandle}, DoStuff) ->
  fdb_raw:transact(DB, DoStuff).

encode(Select = #select{ gt  = GT0
                       , gte = GTE0
                       , lt  = LT0
                       , lte = LTE0
                       }) ->
  Select#select{ gt  = encode(GT0 )
               , gte = encode(GTE0)
               , lt  = encode(LT0 )
               , lte = encode(LTE0)
               };
encode(nil) -> nil;
encode(Key) -> tuple:pack(Key).
