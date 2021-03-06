-module(fdb_ranked).
-export([ clear/2
        , get/2
        , get_nth/2
        , get_nth/3
        , get_range/2
        , get_rank/2
        , get_rank/3
        , get_size/1
        , open/2
        , set/3
        ]).

-export([debug_print/1]).
-export([trest/0]).
-export([verify_db/2]).

%% see https://foundationdb.com/recipes/developer/ranked-sets
%% or  http://en.wikipedia.org/wiki/Skip_list
%% for explanation

-include("../../include/fdb.hrl").

-define(MAX_LEVELS, 8).
-define(LEVEL_FAN_POW, 2).

%% data model:
%%   level  0              - original values
%%   levels 1 - MAX_LEVELS - number of skipped values from level 0

-spec open(fdb_handle(), term()) -> term().
open(Handle,Prefix) when not is_binary(Prefix) ->
  open(Handle, tuple:pack(Prefix));
open(Handle,Prefix)                        ->
  RankedSet = {ranked_set, Prefix, Handle},
  setup_levels(RankedSet),
  RankedSet.

setup_levels({ranked_set, Prefix, DB = {db,_}}) ->
  fdb_raw:transact(DB, fun(Tx) -> setup_levels({ranked_set, Prefix, Tx}) end);
setup_levels(Handle = {ranked_set, _Prefix, {tx,_}}) ->
  lists:foreach( fun(Level) -> setup_level(Handle, Level) end
               , lists:seq(1,?MAX_LEVELS)
               ).

setup_level({ranked_set, Prefix, Tx}, Level) ->
  LevelHeadKey = <<Prefix/binary, Level:8>>,
  case fdb_raw:get(Tx, LevelHeadKey) of
    {ok, _Value} -> ok;
    not_found    -> fdb_raw:set(Tx, LevelHeadKey, term_to_binary(0))
  end.

slow_count(Tx, Prefix, Level, BeginKey, EndKey) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, Level:8>>),
  %% fdb_subspace does include the prefix key
  case Level of
    0 ->
      L = fdb_subspace:get_range(Subspace, #select{ gt = BeginKey, lte = EndKey}),
      length(L);  %% the <<>> is not stored in level 0
    _ ->
      L = fdb_subspace:get_range(Subspace, #select{ gte = BeginKey, lt = EndKey}),
      lists:sum([ V || {_K, V} <- L ])
  end.

get({ranked_set, Prefix, Handle}, Key) ->
  %% actual values are in level 0
  Subspace = fdb_subspace:open(Handle, <<Prefix/binary, 0:8>>),
  fdb_subspace:get(Subspace, Key).

get_range({ranked_set, Prefix, Handle}, Select = #select{}) ->
  %% actual values are in level 0
  Subspace = fdb_subspace:open(Handle, <<Prefix/binary, 0:8>>),
  fdb_subspace:get_range(Subspace, Select).

set({ranked_set, Prefix, DB = {db,_}}, Key, Value) ->
  fdb_raw:transact(DB, fun(Tx) -> set({ranked_set, Prefix, Tx}, Key, Value) end);
set({ranked_set, Prefix, Tx = {tx,_}}, Key, Value) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, 0:8>>),
  case fdb_subspace:get(Subspace, Key) of
    {ok, _Value} ->
      %% if the value already exists, then the order/rank will not change
      fdb_subspace:set(Subspace, Key, Value);
    not_found    ->
      fdb_subspace:set(Subspace, Key, Value),
      Hash = erlang:phash2(Key),
      lists:foreach( fun(Level) ->
                       add_to_level(Tx, Prefix, Level, Key, Hash)
                     end
                   , lists:seq(1, ?MAX_LEVELS)
                   )
  end.

clear({ranked_set, Prefix, DB = {db,_}}, Key) ->
  fdb_raw :transact(DB, fun(Tx) -> clear({ranked_set, Prefix, Tx}, Key) end);
clear({ranked_set, Prefix, Tx = {tx,_}}, Key) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, 0:8>>),
  case fdb_subspace:get(Subspace, Key) of
    not_found    ->
      %% if the value was not there, we are done
      ok;
    {ok, _Value} ->
      fdb_subspace:clear(Subspace, Key),
      Hash = erlang:phash2(Key),
      lists:foreach( fun(Level) ->
                       remove_from_level(Tx, Prefix, Level, Key, Hash)
                     end
                   , lists:seq(1, ?MAX_LEVELS)
                   )
  end.

get_rank(Ranked, Key) -> get_rank(Ranked, Key, []).

get_rank({ranked_set, Prefix, DB = {db,_}}, Key,Options) ->
  fdb_raw:transact(DB, fun(Tx) -> get_rank({ranked_set, Prefix, Tx}, Key, Options) end);
get_rank(Ranked = {ranked_set, Prefix, Tx = {tx,_}}, Key, Options) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, 0:8>>),
  case fdb_subspace:get(Subspace, Key) of
    not_found    ->
      not_found;
    {ok, _Value} ->
      %% we start at the top level, counting all items starting from 1
      %% first item <<>> is not counted
      Rank = acc_rank_in_levels(Tx, Prefix, ?MAX_LEVELS, 0, <<>>, Key),
      case lists:member(is_reverse, Options) of
        false -> {ok, Rank};
        true  -> {ok, get_size(Ranked) - Rank + 1}
      end
  end.

acc_rank_in_levels(Tx, Prefix,     0, Rank, CurrKey, TargetKey) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, 0:8>>),
  L = fdb_subspace:get_range(Subspace, #select{ gt = CurrKey, lte = TargetKey}),
  Rank + length(L);
acc_rank_in_levels(Tx, Prefix, Level, Rank, CurrKey0, TargetKey) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, Level:8>>),
  L = fdb_subspace:get_range(Subspace, #select{ gte = CurrKey0, lte = TargetKey }),
  RevL = lists:reverse(L),
  %% we didn't skip all items counted in the last item, so we cannot count that one
  SkipSum = lists:sum([ V || {_K, V} <- tl(RevL) ]),
  case hd(RevL) of
    {TargetKey,_} ->
      %% the item we are looking for was already in this level, we don't have to go lower
      Rank + SkipSum;
    {CurrKey1 ,_} ->
      %% we have to count the rank in greater detail
      acc_rank_in_levels(Tx, Prefix, Level-1, Rank + SkipSum, CurrKey1, TargetKey)
  end.

get_size({ranked_set, Prefix, DB = {db,_}}) ->
  fdb_raw:transact(DB, fun(Tx) -> get_size({ranked_set, Prefix, Tx}) end);
get_size({ranked_set, Prefix, Tx = {tx,_}}) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, ?MAX_LEVELS:8>>),
  L = fdb_subspace:get_range(Subspace, nil, nil),
  lists:sum([ V || {_K, V} <- L ]).

add_to_level(Tx, Prefix, Level, Key, Hash) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, Level:8>>),
  %% at least the Key == <<>> must be in level
  {ok, {PrevKey, PrevCount}} = fdb_subspace:previous(Subspace, Key),
  IsSkipped = (Hash band ((1 bsl (Level * ?LEVEL_FAN_POW)) - 1)) /= 0,
  case IsSkipped of
    true  ->
      fdb_subspace:set(Subspace, PrevKey, PrevCount + 1);
    false ->
      NewPrevCount = slow_count(Tx, Prefix, Level - 1, PrevKey, Key),
      Count = PrevCount - NewPrevCount + 1,
      fdb_subspace:set(Subspace, PrevKey, NewPrevCount),
      fdb_subspace:set(Subspace, Key    , Count       )
  end.

remove_from_level(Tx, Prefix, Level, Key, Hash) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, Level:8>>),
  %% at least the Key == <<>> must be in level
  {ok, {PrevKey, PrevCount}} = fdb_subspace:previous(Subspace, Key),
  IsSkipped = (Hash band ((1 bsl (Level * ?LEVEL_FAN_POW)) - 1)) /= 0,
  case IsSkipped of
    true  ->
      fdb_subspace:set(Subspace, PrevKey, PrevCount - 1);
    false ->
      {ok, Count} = fdb_subspace:get(Subspace, Key),
      fdb_subspace:set(Subspace, PrevKey, PrevCount + Count - 1),
      fdb_subspace:clear(Subspace, Key)
  end.

get_nth(Ranked, N) -> get_nth(Ranked, N, []).

get_nth({ranked_set, Prefix, DB = {db,_}}, N, Options) ->
  fdb_raw:transact(DB, fun(Tx) ->
    get_nth({ranked_set, Prefix, Tx}, N, Options)
  end);
get_nth(Ranked = {ranked_set, Prefix, Tx = {tx,_}}, N, Options) ->
  Size = get_size(Ranked),
  case (N=<0) or (Size<N) of
    true  -> not_found;
    false -> case lists:member(is_reverse, Options) of
      false -> get_nth_from_level(Tx, Prefix, <<>>, nil, N, ?MAX_LEVELS);
      %% the rank is 1-based, so in list [a], a is 1st from beginning and size - 1 + 1
      %% in reversed list
      true  -> get_nth_from_level(Tx, Prefix, <<>>, nil, Size-N+1, ?MAX_LEVELS)
    end
  end.

get_nth_from_level(Tx, Prefix, CurrKey, LastKey, N, 0) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, 0:8>>),
  L = fdb_subspace:get_range(Subspace, #select{ gt = CurrKey, lte = LastKey}),
  %% CurrKey would be rank 0
  {K,_V} = lists:nth(N, L),
  {ok, K};
get_nth_from_level(Tx, Prefix, CurrKey, LastKey, N, Level) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, Level:8>>),
  L = fdb_subspace:get_range(Subspace, #select{ gte = CurrKey, lte = LastKey }),
  go_deeper(Tx, Prefix, Level, N, L).

go_deeper(Tx, Prefix, Level, N, [{K1,_V1}]) ->
  get_nth_from_level(Tx, Prefix, K1, nil, N, Level - 1);
%% N is never 0 and K1 is never the key we are looking for
go_deeper(Tx, Prefix, Level, N, [{K1,V1}, {K2, V2} | Tail]) ->
  case {N < V1, N=:=V1} of
    %% go lower level
    {true , false} -> get_nth_from_level(Tx, Prefix, K1, K2, N, Level - 1);
    %% no need to go lower level
    {false, true } -> {ok, K2};
    %% not there yet, shorten the distance only
    {false, false} -> go_deeper(Tx, Prefix, Level, N-V1, [{K2, V2} | Tail])
  end.

debug_print(Ranked = {ranked_set, Prefix, Handle}) ->
  Subspace = fdb_subspace:open(Handle, <<Prefix/binary, 0>>),
  Keys = [ K || {K, _V} <- fdb_subspace:get_range(Subspace, nil, nil) ],
  lists:foreach(fun(K) -> debug_print_key(Ranked, K) end, [<<>>|Keys]).

debug_print_key(Ranked, Key) ->
  Term = {Key, lists:append([  key_level(Ranked, Key, Level)
                            || Level <- lists:seq(1, ?MAX_LEVELS)
                            ])},
  io:format(user,"~p~n", [Term]).

key_level({ranked_set, Prefix, Handle}, Key, Level) ->
  Subspace = fdb_subspace:open(Handle, <<Prefix/binary, Level:8>>),
  case fdb_subspace:get(Subspace, Key) of
    not_found -> [];
    {ok, Count} -> [Count]
  end.

trest() ->
  {ok, DB} = fdb_raw:init_and_open(),
  trest(DB, {[], []}).

trest(DB, {InDB0, PastActions}) ->
  io:format(".",[]),
  New = random:uniform(10000),
  Action = case lists:member(New, InDB0) of
    true -> remove;
    false -> add
  end,
  InDB1 = case Action of
    add -> [New|InDB0];
    remove -> InDB0 -- [New]
  end,
  InDB2 = lists:sort(InDB1),
  try
    run_action(DB, Action, New)
    %% verify_db(DB, InDB2)
  catch
    _:Error ->
      io:format("Caught error: ~p~n~p~n", [Error,erlang:get_stacktrace()]),
      io:format("PastActions:~n~pLastAction:~n~p", [PastActions,{Action,New}]),
      erlang:error(unexpected_error)
  end,
  trest(DB, {InDB2, [{Action, New}|PastActions]}).

run_action(DB, remove, New) ->
  ok = fdb_ranked:clear(fdb_ranked:open(DB, {<<"__test">>}), New);
run_action(DB, add, New) ->
  ok = fdb_ranked:set(fdb_ranked:open(DB, {<<"__test">>}), New, New).

verify_db(DB, InDB) ->
  InDBWithIndex = lists:zip(InDB, lists:seq(1,length(InDB))),
  [  {ok,Index} = fdb_ranked:get_rank(fdb_ranked:open(DB,{<<"__test">>}), Item)
  || {Item, Index} <- InDBWithIndex ].
