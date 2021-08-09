%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(influxdb_line).

-define(backslash, <<"\\">>).
-define(comma, <<",">>).
-define(double_quote, <<"\"">>).
-define(equal_sign, <<"=">>).
-define(space, <<" ">>).

-export([generate_point/1]).

-export([ encode/1
        , encode/2
        ]).

encode(Point) ->
    encode(v1, Point).

encode(v2, Point) ->
    influxdb_line_v2:encode(Point);

encode(v1, Point) when is_map(Point) ->
    encode([Point]);
encode(v1, Points) when is_list(Points) ->
    try encode_([generate_point(Point) || Point <- Points]) of
        Encoded -> Encoded
    catch
        error : Reason ->
            {error, Reason}
    end.

encode_(Points) when is_list(Points), length(Points) > 0 ->
    lists:foldr(fun(Point, Acc) when is_map(Point) ->
                    [encode_(Point) | Acc]
                end, [], Points);

encode_(Point = #{measurement := Measurement, fields := Fields}) ->
    [encode_measurement(Measurement),
      encode_tags(maps:get(tags, Point, #{})),
      " ", encode_fields(Fields),
      case maps:get(timestamp, Point, undefined) of
          undefined -> [];
          Timestamp -> [" ", encode_timestamp(Timestamp)]
      end,
      "\n"];

encode_(_Point) ->
    error(invalid_point).

encode_measurement(Measurement) ->
    escape_special_chars(measurement, to_binary(Measurement)).

encode_fields(Fields) when is_map(Fields) ->
    encode_fields(maps:to_list(Fields));
encode_fields([]) ->
    error(missing_field);
encode_fields([{Key, Value}]) ->
    encode_field(Key, Value);
encode_fields([{Key, Value} | Rest]) ->
    [encode_field(Key, Value), ",", encode_fields(Rest)].

encode_field(Key, Value) ->
    [escape_special_chars(field_key, to_binary(Key)), "=", encode_field_value(Value)].

encode_field_value(Value) when is_integer(Value) ->
    Int = erlang:integer_to_binary(Value),
    <<Int/binary, "i">>;
encode_field_value(Value) when is_float(Value) ->
    erlang:float_to_binary(Value, [compact, {decimals, 12}]);
encode_field_value(Value) when is_atom(Value) ->
    if
        Value =:= t; Value =:= 'T'; Value =:= true; Value =:= 'True'; Value =:= 'TRUE' -> <<"t">>;
        Value =:= f; Value =:= 'F'; Value =:= false; Value =:= 'False'; Value =:= 'FALSE' -> <<"f">>;
        true ->
            encode_field_value(to_binary(Value))
    end;
encode_field_value(Value) ->
    Bin = escape_special_chars(field_value, to_binary(Value)),
    <<?double_quote/binary, Bin/binary, ?double_quote/binary>>.

encode_tags(Tags) when is_map(Tags) ->
    encode_tags(maps:to_list(Tags));
encode_tags([]) ->
    [];
encode_tags([{Key, Value}]) ->
    encode_tag(Key, Value);
encode_tags([{Key, Value} | Rest]) ->
    [encode_tag(Key, Value), encode_tags(Rest)].

encode_tag(Key, Value) ->
    [",", escape_special_chars(tag_key, to_binary(Key)), "=", escape_special_chars(tag_value, to_binary2(Value))].

encode_timestamp(Timestamp) when is_integer(Timestamp) ->
    erlang:integer_to_binary(Timestamp).

escape_special_chars(field_value, String) when is_binary(String) ->
    escape_special_chars([?double_quote], String);
escape_special_chars(measurement, String) when is_binary(String) ->
    escape_special_chars([?comma, ?space], String);
escape_special_chars(Element, String)
  when is_binary(String), Element =:= tag_key; Element =:= tag_value; Element =:= field_key ->
    escape_special_chars([?comma, ?equal_sign, ?space], String);

escape_special_chars(Pattern, String) when is_list(Pattern) ->
    binary:replace(String, Pattern, <<"\\">>, [global, {insert_replaced, 1}]).

to_binary(Data) when is_binary(Data) ->
    Data;
to_binary(Data) when is_list(Data) ->
    erlang:list_to_binary(Data);
to_binary(Data) when is_atom(Data) ->
    erlang:atom_to_binary(Data, utf8);
to_binary(_) ->
    error(invalid_type).

to_binary2(Data) when is_integer(Data) ->
    erlang:integer_to_binary(Data);
to_binary2(Data) when is_float(Data) ->
    erlang:float_to_binary(Data, [{decimals, 8}, compact]);
to_binary2(Data) ->
    to_binary(Data).

generate_point(Point) when is_map(Point) ->
    Fun =
        fun(Key, Value, CurrentPoint) ->
            CurrentPoint#{point_key(Key) => Value}
        end,
    maps:fold(Fun, #{}, Point).

point_key(Key) when is_atom(Key) -> Key;
point_key(<<"measurement">>) -> measurement;
point_key(<<"tags">>) -> tags;
point_key(<<"fields">>) -> fields;
point_key(<<"timestamp">>) -> timestamp.
