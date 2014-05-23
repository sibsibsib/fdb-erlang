-module(fdb_subspace_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("../include/fdb.hrl").

-define(SOLIB,"../priv/fdb_nif").

basic_test()->
  {ok,DB} = fdb_raw:init_and_open_try_5_times(?SOLIB),
  Space  = fdb_subspace:open(DB,<<"__test">>),
  fdb_subspace:clear_range(Space, nil, nil),
  fdb_subspace:set(Space, "key", "K"),
  SpaceA = fdb_subspace:open(DB,<<"__test_a">>),
  SpaceB = fdb_subspace:open(DB,<<"__test_b">>),
  fdb_subspace:set(SpaceA, "key", "A"),
  fdb_subspace:set(SpaceB, "key", "B"),
  ?assertEqual({ok, "A"}, fdb_subspace:get(SpaceA, "key")),
  ?assertEqual({ok, "B"}, fdb_subspace:get(SpaceB, "key")),
  ?assertEqual( [{"key","B"}]
              , fdb_subspace:get_range(SpaceB, nil, nil)),
  fdb_subspace:clear(SpaceA, "key"),
  fdb_subspace:clear(SpaceB, "key"),
  ?assertEqual(not_found, fdb_subspace:get(SpaceA, "key")),
  ?assertEqual(not_found, fdb_subspace:get(SpaceB, "key")),
  ?assertEqual( []
              , fdb_subspace:get_range(SpaceB, nil, nil)).

invalid_handle_test() ->
  ?assertError(invalid_fdb_handle, fdb_subspace:open({ok, foo} ,<<"__test">>)),
  ?assertError(invalid_fdb_handle, fdb_subspace:open(<<"foo">> ,<<"__test">>)).

range_test() ->
  {ok, DB} = fdb_raw:init_and_open_try_5_times(?SOLIB),
  range_test_core(DB).

range_test_core(Handle) ->
  Subspace = fdb_subspace:open(Handle, <<"__test">>),
  fdb_subspace:clear_range(Subspace, nil, nil),
  [ok = fdb_subspace:set(Subspace, 2*I, 2*I) || I <- lists:seq(1, 9)],
  %% test closed interval, when values are present
  ?assertEqual( [ {2, 2}
                , {4, 4}
                , {6, 6}
                ]
              , fdb_subspace:get_range(Subspace, #select{gte = 2, lte = 6})),
  %% test closed interval, when values are not present
  ?assertEqual( [ {4, 4}
                , {6, 6}
                ]
              , fdb_subspace:get_range(Subspace, #select{gte = 3, lte = 7})),
  %% test open interval, when values are present
  ?assertEqual( [ {6, 6}
                , {8, 8}
                ]
              , fdb_subspace:get_range(Subspace, #select{gt = 4, lt = 10})),
  %% test open interval, when values are not present
  ?assertEqual( [ {4, 4}
                , {6, 6}
                ]
              , fdb_subspace:get_range(Subspace, #select{gt = 3, lt = 7})),
  %% test interval, with endpoint offsets
  ?assertEqual( [ {2, 2}
                , {4, 4}
                , {6, 6}
                , {8, 8}
                , {10, 10}
                ]
              , fdb_subspace:get_range(Subspace, #select{ gt           = 3
                                                        , offset_begin = -1
                                                        , lte          = 7
                                                        , offset_end   = 2
                                                        })),
  %% test getting data around a single value
  ?assertEqual( [ {2, 2}
                , {4, 4}
                , {6, 6}
                , {8, 8}
                , {10, 10}
                ]
              , fdb_subspace:get_range(Subspace, #select{ gte          = 6
                                                        , offset_begin = -2
                                                        , lte          = 6
                                                        , offset_end   = 2
                                                        })),
  %% test range/3
  ?assertEqual( [ {4, 4}
                , {6, 6}
                ]
              , fdb_subspace:get_range(Subspace, 4, 7)),
  fdb_subspace:clear_range(Subspace, nil, nil).

