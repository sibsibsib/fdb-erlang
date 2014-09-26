-module(fdb_test).

-include_lib("eunit/include/eunit.hrl").
-include("../include/fdb.hrl").

-define(SOLIB,"../priv/fdb_nif").

hello_world_test() ->
  {ok, DB} = fdb_raw:init_and_open([{so_file,?SOLIB}, {num_retries, 5}]),
  AKey = <<"Hello">>,
  AValue = <<"World">>,
  ok = fdb:set(DB, AKey, AValue),
  {ok, AValue} = fdb:get(DB, AKey),
  ok = fdb:clear(DB, AKey),
  ?assertEqual(not_found, fdb:get(DB, AKey)).

transaction_test() ->
  {ok, DB} = fdb_raw:init_and_open([{so_file,?SOLIB}, {num_retries, 5}]),
  AKey = <<"abc">>,
  AValue = <<"xyz">>,
  ok = fdb:clear(DB, AKey),
  not_found = fdb:get(DB, AKey),
  fdb:transact(DB, fun(Tx)->
        fdb:set(Tx, AKey, AValue)
    end),
  ?assertEqual({ok, AValue}, fdb:get(DB, AKey)),
  ok = fdb:clear(DB, AKey).

range_test() ->
  {ok, DB} = fdb_raw:init_and_open([{so_file,?SOLIB}, {num_retries, 5}]),
  fdb_raw:clear_range(DB, <<0>>, <<255:8>>),
  fdb:transact(DB, fun(Tx) ->
                       [ok = fdb:set(Tx, I, I) || I <- lists:seq(1, 9)]
                   end),
  ?assertEqual([{2, 2}, {3, 3},{4, 4}], fdb:get_range(DB, #select{gte = 2, lte = 4})),
  ?assertEqual([{2, 2}, {3, 3}], fdb:get_range(DB, #select{ gte = 2, lte =3})),
  ?assertEqual([{2, 2}, {3, 3}], fdb:get_range(DB, #select{ gt = 1, lt = 4})),
  ?assertEqual([{8, 8}, {9, 9}], fdb:get_range(DB, #select{ gt = 7, lt = 10})),
  ?assertEqual([{1, 1}, {2, 2}], fdb:get_range(DB, #select{ gt = 0, lt = 3})),
  ?assertEqual([{2, 2}, {3, 3}], fdb:get_range(DB, 2, 4)),
  ?assertEqual([{1, 1}, {2, 2}], fdb:get_range(DB, #select{ gt=0, lt = 3})),
  ?assertEqual([{1, 1}, {2, 2}], fdb:get_range(DB, #select{ gt=0, lt = 3})),
  ?assertEqual([{8, 8}, {9, 9}], fdb:get_range(DB, #select{ gt=7})),
  ?assertEqual([{8, 8}, {9, 9}], fdb:get_range(DB, #select{ gte = 8})),
  fdb:clear_range(DB, 4, 8),
  ?assertEqual([{3, 3}, {8, 8}], fdb:get_range(DB, #select{ gt = 2, lt = 9})),
  fdb:clear_range(DB, 1, 9).

range_single_transaction_test() ->
  {ok, DB} = fdb_raw:init_and_open([{so_file,?SOLIB}, {num_retries, 5}]),
  fdb_raw:clear_range(DB, <<0>>, <<255:8>>),
  fdb:transact(DB, fun(Tx) ->
   [ok = fdb:set(Tx, I, I) || I <- lists:seq(1, 9)],
    ?assertEqual([{2, 2}, {3, 3},{4, 4}], fdb:get_range(Tx, #select{gte = 2, lte = 4})),
    ?assertEqual([{2, 2}, {3, 3}], fdb:get_range(Tx, #select{ gte = 2, lte =3})),
    ?assertEqual([{2, 2}, {3, 3}], fdb:get_range(Tx, #select{ gt = 1, lt = 4})),
    ?assertEqual([{8, 8}, {9, 9}], fdb:get_range(Tx, #select{ gt = 7, lt = 10})),
    ?assertEqual([{1, 1}, {2, 2}], fdb:get_range(Tx, #select{ gt = 0, lt = 3})),
    ?assertEqual([{2, 2}, {3, 3}], fdb:get_range(Tx, 2, 4)),
    ?assertEqual([{1, 1}, {2, 2}], fdb:get_range(Tx, #select{ gt=0, lt = 3})),
    ?assertEqual([{1, 1}, {2, 2}], fdb:get_range(Tx, #select{ gt=0, lt = 3})),
    ?assertEqual([{8, 8}, {9, 9}], fdb:get_range(Tx, #select{ gt=7})),
    ?assertEqual([{8, 8}, {9, 9}], fdb:get_range(Tx, #select{ gte = 8})),
    fdb:clear_range(Tx, 4, 8),
    ?assertEqual([{3, 3}, {8, 8}], fdb:get_range(Tx, #select{ gt = 2, lt = 9}))
                   end),
  fdb:clear_range(DB, 1, 9).
