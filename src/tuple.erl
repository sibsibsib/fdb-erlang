-module(tuple).
-export([pack/1, unpack/1]).

%% this implements a subset of pythons fdb tuple implementation
%% only binary and integer tuples are supported
%% Python None and unicode string are not implemented

-define(TERMINATOR,   0).
-define(BINARY    ,   1).
-define(STRING    ,   2). %% lists of integers
-define(INTEGER   ,  20). %% integer spans from 11 to 29
-define(ESCAPE    , 255).

pack(Tuple) when is_tuple(Tuple) ->
  << <<(pack_value(Value))/binary>> || Value <- tuple_to_list(Tuple) >>;
pack(Value) when is_list(Value) or is_integer(Value) or is_binary(Value) ->
  pack_value(Value).

pack_value(Binary) when is_binary(Binary) ->
  << ?BINARY
  ,  (escape_bin(Binary))/binary
  ,  ?TERMINATOR
  >>;
pack_value(List) when is_list(List) ->
  << ?STRING
  ,  (escape_bin(list_to_binary(List)))/binary
  ,  ?TERMINATOR
  >>;
pack_value(0) ->
  <<?INTEGER>>;
pack_value(Integer) when is_integer(Integer) ->
  pack_integer(Integer).

unpack(Binary) when is_binary(Binary) ->
  Tuple = unpack(Binary, []),
  case Tuple of
    {Value} -> Value;
    _       -> Tuple
  end.

unpack(<<>>, UnpackedValues) ->
  list_to_tuple(lists:reverse(UnpackedValues));
unpack(<<?BINARY, Rest0/binary>>, UnpackedValues) ->
  {BinValue, Rest1} = unpack_until_terminator(Rest0, 0),
  unpack(Rest1, [unescape_bin(BinValue)|UnpackedValues]);
unpack(<<?STRING, Rest0/binary>>, UnpackedValues) ->
  {BinValue, Rest1} = unpack_until_terminator(Rest0, 0),
  unpack(Rest1, [binary_to_list(unescape_bin(BinValue))|UnpackedValues]);
unpack(<<?INTEGER, Rest/binary>>, UnpackedValues) ->
  unpack(Rest, [0|UnpackedValues]);
unpack(<<IntCode:8, Rest0/binary>>, UnpackedValues) when ((?INTEGER - 9) =< IntCode)
                                                     and (IntCode =< (?INTEGER + 9)) ->
  BitSize = 8 * abs(IntCode - ?INTEGER),
  <<Value0:BitSize, Rest1/binary>> = Rest0,
  Value1 = case IntCode - ?INTEGER > 0 of
    true  -> Value0;
    false -> Value0 - (1 bsl BitSize)
  end,
  unpack(Rest1, [Value1|UnpackedValues]).

unpack_until_terminator(Binary, StartPos) ->
  RestLen = byte_size(Binary)-StartPos,
  {Pos1,1} = binary:match(Binary, <<?TERMINATOR>>        , [{scope,{StartPos, RestLen}}]),
  Result   = binary:match(Binary, <<?TERMINATOR,?ESCAPE>>, [{scope,{StartPos, RestLen}}]),
  case Result of
    {Pos1,2} ->
      %% this terminator is escaped, search for next one
      unpack_until_terminator(Binary, Pos1+2);
    _ ->
      %% further position or nomatch
      %% Value without the terminator
      Value = binary:part(Binary, 0       , Pos1),
      %% Rest after the terminator
      Rest  = binary:part(Binary, Pos1 + 1, byte_size(Binary) - Pos1 - 1),
      {Value, Rest}
  end.

unescape_bin(Binary) ->
 binary:replace(Binary, <<?TERMINATOR,?ESCAPE>>, <<?TERMINATOR>>, [global]).

escape_bin(Binary) ->
 binary:replace(Binary, <<?TERMINATOR>>, <<?TERMINATOR,?ESCAPE>>, [global]).

pack_integer(Int) ->
  Size = int_size(Int),
  <<(?INTEGER+Size),(pack_integer(Int, Size))/binary>>.

pack_integer(Int, Size) when Size<0 -> pack_integer((1 bsl ((-Size)*8))+Int, -Size);
pack_integer(Int, Size)             -> <<Int:(Size*8)>>.

%% use pattern matching as opposed to a list, so it's faster I assume
int_size(Int) when Int < 0 -> -int_size(-Int);
int_size(Int) when Int < 16#100 -> 1;
int_size(Int) when Int < 16#10000 -> 2;
int_size(Int) when Int < 16#1000000 -> 3;
int_size(Int) when Int < 16#100000000 -> 4;
int_size(Int) when Int < 16#10000000000 -> 5;
int_size(Int) when Int < 16#1000000000000 -> 6;
int_size(Int) when Int < 16#100000000000000 -> 7;
int_size(Int) when Int < 16#10000000000000000 -> 8;
int_size(Int) when Int < 16#1000000000000000000 -> 9.
