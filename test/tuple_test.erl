-module(tuple_test).

-include_lib("proper/include/proper.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([ prop_pack_unpack/0
        , prop_pack_ordering/0
        , prop_pack_concat/0
        ]).

%% this is to show the proper output when running from eunit
-define( SHOW_PROPER_OUTPUT(Exp)
       , begin
           Leader = erlang:group_leader(),
           erlang:group_leader(whereis(user), self()),
           Result = Exp,
           erlang:group_leader(Leader, self()),
           Result
         end).

proper_test_() ->
  [ ?_assert(?SHOW_PROPER_OUTPUT(proper:quickcheck(prop_pack_unpack())))
  , ?_assert(?SHOW_PROPER_OUTPUT(proper:quickcheck(prop_pack_ordering())))
  , ?_assert(?SHOW_PROPER_OUTPUT(proper:quickcheck(prop_pack_concat())))
  ].

prop_pack_unpack() ->
  ?FORALL( List
         , list(union(integer(),binary(),list(range(0,255))))
         , begin
             Tuple = my_list_to_tuple(List),
             Tuple == tuple:unpack(tuple:pack(Tuple))
           end).

%% ordering property works only when encoding single values
%% it is impossible it work for all, because {_,_} < {_,_,_}
%% regardless of what the tuple contains
prop_pack_ordering() ->
  ?FORALL( {ValA, ValB}
         , union( {integer(), integer()}
                , {binary() , binary() }
                , {list(range(0,255)), list(range(0,255))})
         , begin
             (ValA =< ValB) == (tuple:pack({ValA}) =< tuple:pack({ValB}))
           end).

%% this shows that subspaces work
%% even when we mix tuples and single values
prop_pack_concat() ->
  ?FORALL( {ListA, ListB}
         , { list(union(integer(),binary(),list(range(0,255))))
           , list(union(integer(),binary(),list(range(0,255))))}
         , begin
             TupleA  = my_list_to_tuple(ListA),
             TupleB  = my_list_to_tuple(ListB),
             TupleAB = my_list_to_tuple(ListA ++ ListB),
             tuple:pack(TupleAB) == list_to_binary([ tuple:pack(TupleA)
                                                   , tuple:pack(TupleB) ])
           end).

my_list_to_tuple([Value]) -> Value;
my_list_to_tuple(List)    -> list_to_tuple(List).

