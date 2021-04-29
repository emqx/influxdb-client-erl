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
-module(influxdb_http).

-export([ is_alive/1
        , write/2
        , write/3]).

is_alive(#{pool := Pool}) ->
    Path = "/ping",
    Headers = [{<<"verbose">>, <<"true">>}],
    try
        Worker = ehttpc_pool:pick_worker(Pool),
        case ehttpc:request(Worker, get, {Path, Headers}) of
            {ok, 204, _} ->
                true;
            {ok, 204, _, _} ->
                true;
            _ ->
                false
        end
    catch _E:_R:_S ->
        false
    end.

write(#{pool := Pool, path := Path}, Data) ->
    do_write(ehttpc_pool:pick_worker(Pool), Path, Data).

write(#{pool := Pool, path := Path}, Key, Data) ->
    do_write(ehttpc_pool:pick_worker(Pool, Key), Path, Data).

do_write(Worker, Path, Data) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    try ehttpc:request(Worker, post, {Path, Headers, Data}) of
        {ok, 204, _} ->
            ok;
        {ok, 204, _, _} ->
            ok;
        {ok, StatusCode, Reason} ->
            {fail, {StatusCode, Reason}};
        {ok, StatusCode, Reason, Body} ->
            {fail, {StatusCode, Reason, Body}};
        Error ->
            {fail, Error}
    catch E:R:S ->
        logger:error("[InfluxDB] http write fail: ~0p ~0p ~0p", [E, R, S]),
        {fail, {E, R}}
    end.
