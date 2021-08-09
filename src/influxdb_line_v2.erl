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
-module(influxdb_line_v2).

-export([encode/1]).

-include("influxdb.hrl").

encode(Points) when is_list(Points) ->
    [encode(influxdb_line:generate_point(Point)) || Point <- Points];

encode(Point = #{measurement := M, tags := Ts, fields := Fs}) ->
    Tags = encode_tags(Ts),
    Fields = encode_fields(Fs),
    Timestamp = time_stamp(Point),
    [M, <<",">>, Tags, <<" ">>, Fields, Timestamp, <<"\n">>].

encode_tags(Tags) ->
    encode_kvs(Tags).

encode_fields(Fields) ->
    encode_kvs(Fields).

encode_kvs(M) when is_map(M) ->
    encode_kvs(maps:to_list(M), []);
encode_kvs(L) ->
    encode_kvs(L, []).

encode_kvs([{K, V}], Res) ->
    Key = to_binary(K),
    Value = to_binary(V),
    lists:reverse([<<Key/binary, "=", Value/binary>> | Res]);
encode_kvs([{K, V} | KVs], Res) ->
    Key = to_binary(K),
    Value = to_binary(V),
    encode_kvs(KVs, [<<Key/binary, "=", Value/binary, ",">> | Res]).

time_stamp(#{timestamp := T}) ->
    Timestamp = to_binary(T),
    <<" ", Timestamp/binary>>;
time_stamp(_) ->
    <<"">>.

to_binary(D) when is_binary(D) ->
    D;
to_binary(D) when is_atom(D) ->
    atom_to_binary(D, utf8);
to_binary(D) when is_list(D) ->
    list_to_binary(D);
to_binary(D) when is_integer(D) ->
    integer_to_binary(D).

