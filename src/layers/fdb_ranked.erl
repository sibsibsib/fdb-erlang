-module(fdb_ranked).
-export([ clear/2
        , get/2
        , get_range/2
        , get_rank/2
        , get_size/1
        , open/2
        , set/3
        ]).

-export([debug_print/1]).

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
  L = fdb_subspace:get_range(Subspace, #select{ gt = BeginKey, lte = EndKey}),
  case Level of
    0 -> length(L);  %% the <<>> is not stored in level 0
    _ -> lists:sum([ V || {_K, V} <- L ])
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
    {ok, Count} ->
      fdb_subspace:clear(Subspace, Key),
      Hash = erlang:phash2(Key),
      lists:foreach( fun(Level) ->
                       remove_from_level(Tx, Prefix, Level, Key, Count, Hash)
                     end
                   , lists:seq(1, ?MAX_LEVELS)
                   )
  end.

get_rank({ranked_set, Prefix, DB = {db,_}}, Key) ->
  fdb_raw:transact(DB, fun(Tx) -> get_rank({ranked_set, Prefix, Tx}, Key) end);
get_rank({ranked_set, Prefix, Tx = {tx,_}}, Key) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, 0:8>>),
  case fdb_subspace:get(Subspace, Key) of
    not_found    ->
      not_found;
    {ok, _Value} ->
      %% we start at the top level, counting all items starting from 1
      %% first item <<>> is not counted
      {ok, acc_rank_in_levels(Tx, Prefix, ?MAX_LEVELS, 0, <<>>, Key)}
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

-include_lib("eunit/include/eunit.hrl").

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

remove_from_level(Tx, Prefix, Level, Key, Count, Hash) ->
  Subspace = fdb_subspace:open(Tx, <<Prefix/binary, Level:8>>),
  %% at least the Key == <<>> must be in level
  {ok, {PrevKey, PrevCount}} = fdb_subspace:previous(Subspace, Key),
  IsSkipped = (Hash band ((1 bsl (Level * ?LEVEL_FAN_POW)) - 1)) /= 0,
  case IsSkipped of
    true  ->
      fdb_subspace:set(Subspace, PrevKey, PrevCount - 1);
    false ->
      fdb_subspace:set(Subspace, PrevKey, PrevCount + Count - 1),
      fdb_subspace:clear(Subspace, Key)
  end.

debug_print(Ranked = {ranked_set, Prefix, Handle}) ->
  Subspace = fdb_subspace:open(Handle, <<Prefix/binary, 0>>),
  Keys = [ K || {K, _V} <- fdb_subspace:get_range(Subspace, nil, nil) ],
  lists:foreach(fun(K) -> debug_print_key(Ranked, K) end, [<<>>|Keys]).

debug_print_key(Ranked, Key) ->
  Term = {Key, lists:append([  key_level(Ranked, Key, Level)
                            || Level <- lists:seq(1, ?MAX_LEVELS)
                            ])},
  io:format("~p~n", [Term]).

key_level({ranked_set, Prefix, Handle}, Key, Level) ->
  Subspace = fdb_subspace:open(Handle, <<Prefix/binary, Level:8>>),
  case fdb_subspace:get(Subspace, Key) of
    not_found -> [];
    {ok, Count} -> [Count]
  end.
