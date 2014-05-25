-module(fdb_ranked_test).

-include_lib("eunit/include/eunit.hrl").
-include("../include/fdb.hrl").

-define(SOLIB,"../priv/fdb_nif").

basic_test()->
  {ok, DB} = fdb_raw:init_and_open_try_5_times(?SOLIB),
  %% clean
  Subspace = fdb_subspace:open(DB,<<"__test">>),
  fdb_subspace:clear_range(Subspace, nil, nil),
  %% init
  Ranked = fdb_ranked:open(DB,<<"__test">>),
  [ok = fdb_ranked:set(Ranked, I, I) || I <- lists:seq(1, 9)],
  %% get_rank
  ?assertEqual( [{2, 2}, {3, 3},{4, 4}]
              , fdb_ranked:get_range(Ranked, #select{gte = 2, lte = 4})),
  ?assertEqual( [  {ok, I}
                || I <- lists:seq(1, 9)
                ]
              , [  fdb_ranked:get_rank(Ranked, I)
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
  %% cleanup
  Subspace = fdb_subspace:open(DB,<<"__test">>),
  fdb_subspace:clear_range(Subspace, nil, nil),
  ok.

