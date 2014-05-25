%% Fdb keys can be atmost 10,000 bytes long
-define(FDB_MAX_KEY_SIZE, 10000).

%% Fdb values can be atmost 100,000 bytes long
-define(FDB_MAX_VALUE_SIZE, 100000).

%% A fdb transaction can take atmost 5 seconds
-define(FDB_MAX_TRANSACTION_TIME_MILLIS, 5000).

%% A fdb transaction cannot write more than
%% 10,000,000 bytes in a transaction.
%% This means key + value must be less than
%% this limit
-define(FDB_MAX_TRANSACTION_SIZE, 10000000).

-record(select, {
   lt = nil, 
   lte = nil, 
   offset_begin = 0, 
   gte = nil, 
   gt = nil, 
   offset_end = 0,
   limit = 0,
   target_bytes = 0,
   streaming_mode = want_all,
   iteration = 1,
   is_snapshot = false,
   is_reverse = false}).

-record(iterator, {
   tx,
   iteration,
   data = [],
   select,
   out_more = true}).

-type fdb_version() :: pos_integer().
-type fdb_errorcode() :: pos_integer().
-type fdb_cmd_result() :: ok | {error, fdb_errorcode()}|{error,nif_not_loaded}.
%-type fdb_qry_result() :: {ok, term()} | {error, fdb_errorcode()}.
-type fdb_database() :: {db, term()}.
-type fdb_transaction() :: {tx, term()}.
-type fdb_handle() :: fdb_database() | fdb_transaction().
-type fdb_key() :: term().
-type fdb_key_value() :: {fdb_key(), term()}.


