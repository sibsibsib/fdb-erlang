-module(fdb_subspace).
-export([ clear/2
        , clear_range/3
        , get/2
        , get_range/2
        , get_range/3
        , next/2
        , open/2
        , previous/2
        , set/3
        ]).

-include("../../include/fdb.hrl").

-type subspace_handle() :: {ss, binary(), fdb_handle()}.

-spec clear(subspace_handle(), fdb_key()) -> ok.
clear({ss, Prefix, H}, K)                       ->
  fdb_raw:clear(H, add_prefix(Prefix, K)).

-spec clear_range(subspace_handle(), fdb_key(), fdb_key()) -> ok.
clear_range(Subspace, nil, End) ->
  clear_range(Subspace, <<>>, End);
clear_range(Subspace, Begin, nil) ->
  %% XXX this is a hack, keys starting with 255 will not be matched
  %%     but it works due to the way tuple is implemented (never starts with 255)
  %% TODO do this properly by increasing the Prefix binary by 1
  clear_range(Subspace, Begin, <<255:8>>);
clear_range({ss, Prefix, Handle}, Begin, End) ->
  fdb_raw:clear_range( Handle
                     , add_prefix(Prefix, Begin)
                     , add_prefix(Prefix, End)
                     ).

-spec get(subspace_handle(), fdb_key()) -> {ok, term()} | not_found.
get({ss, Prefix, Handle}, Key)                         ->
  case fdb_raw:get(Handle, add_prefix(Prefix, Key)) of
    not_found   -> not_found;
    {ok, Value} -> {ok, binary_to_term(Value)}
  end.

get_range({ss,Prefix,Handle}, Select0 = #select{}) ->
  Select1 = add_prefix(Prefix, Select0),
  Select2 = cap_infinity(Prefix, Select1),
  fdb_raw:maybe_do([ fun()  -> fdb_raw:get_range(Handle, Select2) end
                   , fun(L) ->
                       [  {tuple_unpack(remove_prefix(Prefix, K)), binary_to_term(V)}
                       || {K,V} <- L ]
                     end
                   ]).

get_range(Subspace,Begin,End) ->
  get_range(Subspace, #select{gte = Begin, lt = End}).

remove_prefix(Prefix, FullKey) ->
  PrefixSize = size(Prefix),
  <<Prefix:PrefixSize/binary, Key/binary>> = FullKey,
  Key.

tuple_unpack(<<>>)    -> <<>>;
tuple_unpack(Encoded) -> tuple:unpack(Encoded).

-spec next(subspace_handle(), fdb_key()) -> {ok, fdb_key_value()} | not_found.
next({ss, Prefix, Handle}, Key) ->
  PrefixSize = size(Prefix),
  case fdb_raw:next(Handle, add_prefix(Prefix, Key)) of
    not_found ->
      not_found;
    {ok, {<<Prefix:PrefixSize/binary, NextKey/binary>>, NextValue}} ->
      {ok, {tuple:unpack(NextKey), binary_to_term(NextValue)}};
    {ok, _OutsideOfSubspace} ->
      not_found
  end.

-spec previous(subspace_handle(), fdb_key()) -> {ok, fdb_key_value()} | not_found.
previous({ss, Prefix, Handle}, Key) ->
  PrefixSize = size(Prefix),
  case fdb_raw:previous(Handle, add_prefix(Prefix, Key)) of
    not_found ->
      not_found;
    {ok, {<<Prefix:PrefixSize/binary, PrevKey/binary>>, PrevValue}} ->
      %% we can get the <<>> key here, which will not be unpacked
      {ok, {tuple_unpack(PrevKey), binary_to_term(PrevValue)}};
    {ok, _OutsideOfSubspace} ->
      not_found
  end.

-spec open(fdb_handle(), fdb_key()) -> subspace_handle().
open(Handle, Prefix) when not is_binary(Prefix) -> open(Handle, tuple:pack(Prefix));
open(Handle={db, _Database}, Prefix)            -> {ss, Prefix, Handle};
open(Handle={tx, _Database}, Prefix)            -> {ss, Prefix, Handle};
open(_,_) -> erlang:error(invalid_fdb_handle).

-spec set(subspace_handle(), fdb_key(), term()) -> ok.
set({ss,Prefix,H}, K, V) -> fdb_raw:set(H, add_prefix(Prefix, K), term_to_binary(V)).

cap_infinity(Prefix, Select = #select{ gt  = GT0
                                     , gte = GTE0
                                     , lt  = LT0
                                     , lte = LTE0
                                     }) ->
  {GT1, GTE1} = case {GT0, GTE0} of
    %% include the Prefix itself in search by default
    {nil, nil   } -> {nil, Prefix};
    {nil, Prefix} -> {nil, Prefix};
    _             -> {GT0, GTE0}
  end,
  {LT1, LTE1} = case {LT0, LTE0} of
    %% do not include the Prefix itself in search by default
    %% XXX this is a hack, keys starting with 255 will not be matched
    %%     but it works due to the way tuple is implemented (never starts with 255)
    %% TODO do this properly by increasing the Prefix binary by 1
    {nil, nil} -> {<<Prefix/binary, 255:8>>, nil };
    _          -> {LT0                     , LTE0}
  end,
  Select#select{ gt  = GT1
               , gte = GTE1
               , lt  = LT1
               , lte = LTE1
               }.

add_prefix(_Prefix, nil)                        ->
  nil; %% nil in #select stays nil
add_prefix(Prefix, Select = #select{ gt  = GT0
                                   , gte = GTE0
                                   , lt  = LT0
                                   , lte = LTE0
                                   })           ->
  Select#select{ gt  = add_prefix(Prefix, GT0 )
               , gte = add_prefix(Prefix, GTE0)
               , lt  = add_prefix(Prefix, LT0 )
               , lte = add_prefix(Prefix, LTE0)
               };
add_prefix(Prefix, Key) when not is_binary(Key) ->
  add_prefix(Prefix, tuple:pack(Key));
add_prefix(Prefix, Key)                         ->
  <<Prefix/binary, Key/binary>>.


