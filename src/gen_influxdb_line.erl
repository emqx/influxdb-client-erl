-module(gen_influxdb_line).

-define(backslash, <<"\\">>).
-define(comma, <<",">>).
-define(double_quote, <<"\"">>).
-define(equal_sign, <<"=">>).
-define(space, <<" ">>).

-export([
    encode/1,
    encode/2
]).

encode(Points) ->
    encode(Points, []).

encode(Points, Opts) ->
    try encode_(Points, Opts) of
        Encoded -> Encoded
    catch
        error : Reason ->
            {error, Reason}
    end.

encode_(Points, Opts) when is_list(Points), length(Points) > 0 ->
    lists:foldr(fun(Point, Acc) when is_map(Point) ->
                    [encode_(Point, Opts) | Acc]
                end, [], Points);

encode_(Point = #{measurement := Measurement, fields := Fields}, Opts) ->
    Timestamp = case proplists:get_value(set_timestamp, Opts, false) of
                    false -> undefined;
                    true -> erlang:system_time(nanosecond)
                end,
    [encode_measurement(Measurement),
     encode_tags(maps:get(tags, Point, #{})),
     " ", encode_fields(Fields),
     case encode_timestamp(maps:get(timestamp, Point, Timestamp)) of
         undefined -> [];
         Encoded -> [" ", Encoded]
     end,
     "\n"];

encode_(_Point, _Opts) ->
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
    [",", escape_special_chars(tag_key, to_binary(Key)), "=", escape_special_chars(tag_value, to_binary(Value))].

encode_timestamp(Timestamp) when is_integer(Timestamp) ->
    erlang:integer_to_binary(Timestamp);
encode_timestamp(_) ->
    undefined.

escape_special_chars(field_value, String) when is_binary(String) ->
    escape_special_chars([?backslash], String);
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
