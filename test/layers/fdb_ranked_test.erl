-module(fdb_ranked_test).

-include_lib("eunit/include/eunit.hrl").
-include("../include/fdb.hrl").

-define(SOLIB,"../priv/fdb_nif").

basic_test()->
  {ok, DB} = fdb_raw:init_and_open_try_5_times([{so_file,?SOLIB}]),
  %% clean
  Subspace = fdb_subspace:open(DB,<<"__test">>),
  fdb_subspace:clear_range(Subspace, nil, nil),
  %% init
  Ranked = fdb_ranked:open(DB,<<"__test">>),
  [ok = fdb_ranked:set(Ranked, I, I) || I <- lists:seq(1, 9)],
  %% get_range
  ?assertEqual( [{2, 2}, {3, 3},{4, 4}]
              , fdb_ranked:get_range(Ranked, #select{gte = 2, lte = 4})),
  %% get_rank
  ?assertEqual( [  {ok, I}
                || I <- lists:seq(1, 9)
                ]
              , [  fdb_ranked:get_rank(Ranked, I)
                || I <- lists:seq(1, 9)
                ]),
  ?assertEqual( [  {ok, I}
                || I <- lists:seq(9, 1, -1)
                ]
              , [  fdb_ranked:get_rank(Ranked, I, [is_reverse])
                || I <- lists:seq(1, 9)
                ]),
  %% get_size
  ?assertEqual( 9
              , fdb_ranked:get_size(Ranked)),
  %% get_nth
  ?assertEqual( [{ok, 1}, {ok, 2}, {ok, 5}, {ok, 8}, {ok, 9}]
              , [  fdb_ranked:get_nth(Ranked, N)
                || N <- [1,2,5,8,9]
                ]),
  ?assertEqual( [{ok, 9}, {ok, 8}, {ok, 5}, {ok, 2}, {ok, 1}]
              , [  fdb_ranked:get_nth(Ranked, N, [is_reverse])
                || N <- [1,2,5,8,9]
                ]),
  ?assertEqual( not_found
              , fdb_ranked:get_nth(Ranked, 0)),
  ?assertEqual( not_found
              , fdb_ranked:get_nth(Ranked, 10)),
  %% remove some
  ?assertEqual( ok
              , fdb_ranked:clear(Ranked, 3)),
  ?assertEqual( ok
              , fdb_ranked:clear(Ranked, 6)),
  %% get_range after removal
  ?assertEqual( [{2, 2}, {4, 4}]
              , fdb_ranked:get_range(Ranked, #select{gte = 2, lte = 4})),
  %% get_rank after removal
  ?assertEqual( [{ok, 1}, {ok, 2}, {ok, 4}, {ok, 6}, {ok, 7}]
              , [  fdb_ranked:get_rank(Ranked, I)
                || I <- [1,2,5,8,9]
                ]),
  %% get_size after removal
  ?assertEqual( 7
              , fdb_ranked:get_size(Ranked)),
  %% get_nth after removal
  ?assertEqual( [{ok, 1}, {ok, 2}, {ok, 7}, not_found, not_found]
              , [  fdb_ranked:get_nth(Ranked, N)
                || N <- [1,2,5,8,9]
                ]),
  ?assertEqual( [{ok, 9}, {ok, 8}, {ok, 4}, not_found, not_found]
              , [  fdb_ranked:get_nth(Ranked, N, [is_reverse])
                || N <- [1,2,5,8,9]
                ]),
  %% cleanup
  Subspace = fdb_subspace:open(DB,<<"__test">>),
  fdb_subspace:clear_range(Subspace, nil, nil),
  ok.

big_test_core(Size, GetNthOpts, GetExpRank)->
  {ok, DB} = fdb_raw:init_and_open_try_5_times([{so_file,?SOLIB}]),
  %% clean
  Subspace = fdb_subspace:open(DB,<<"__test">>),
  fdb_subspace:clear_range(Subspace, nil, nil),
  %% init
  Ranked = fdb_ranked:open(DB,<<"__test">>),
  [ begin
      %% io:format(user, "Adding item ~p:~n", [I]),
      ok = fdb_ranked:set(Ranked, I, I)
      %% fdb_ranked:debug_print(Ranked)
    end
  || I <- lists:seq(1, Size)],
  Ns = lists:seq(1,Size),
  %% get_range
  Expected = [ {ok, GetExpRank(N)} || N <- Ns ],
  Actual = [ fdb_ranked:get_nth(Ranked, N, GetNthOpts) || N <- Ns],
  %% io:format(user, "Expected:~n~p~nActual:~n~p~n", [Expected, Actual]),
  ?assertEqual( Expected, Actual).

big_1_test_() ->
  { timeout
  , 60
  , fun() ->
      big_test_core( 100
                   , [is_reverse]
                   , fun(N) -> 100 - N + 1 end)
    end
  }.

big_2_test_() ->
  { timeout
  , 60
  , fun() ->
      big_test_core( 100
                   , []
                   , fun(N) -> N end)
    end
  }.
