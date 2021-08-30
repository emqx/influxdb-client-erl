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

do_write(_Worker, Path, Data) ->
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    case request(post, Path, Headers, iolist_to_binary(Data)) of
        {ok, 204} ->
            ok;
        {ok, 204, _Msg} ->
            ok;
        {ok, Code, Msg} ->
            {error, {Code, Msg}};
        {error, Error} ->
            {error, Error}
    end.

    % try ehttpc:request(Worker, post, {Path, Headers, Data}) of
    %     {ok, 204, _} ->
    %         ok;
    %     {ok, 204, _, _} ->
    %         ok;
    %     {ok, StatusCode, Reason} ->
    %         {error, {StatusCode, Reason}};
    %     {ok, StatusCode, Reason, Body} ->
    %         {error, {StatusCode, Reason, Body}};
    %     Error ->
    %         {error, Error}
    % catch E:R:S ->
    %     logger:error("[InfluxDB] http write fail: ~0p ~0p ~0p", [E, R, S]),
    %     {error, {E, R}}
    % end.


request(Method, URL, Headers, Body) ->
    request(Method, URL, Headers, Body, #{}).

request(Method, URL, Headers, Body, Opts) ->
    request(Method, URL, Headers, Body, 3, Opts).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------
request(_Method, URL, _Headers, _Body, 0 = _RetryCnt, _Opts) ->
    {error, timeout};

request(Method, URL, Headers, Body, RetryCnt, Opts0) ->
    Opts = [{pool, default},
            {connect_timeout, 10000}, %% timeout used when establishing the TCP connections
            {recv_timeout, 30000},    %% timeout used when waiting for responses from peer
            {timeout, 150000},        %% how long the connection will be kept in the pool before dropped
            {follow_redirect, true},
            {max_connections, 1024},
            {max_redirect, 5}],
    case timer:tc(hackney, Method, [URL, Headers, Body, Opts]) of
        {RTT, {error, timeout}} ->
            request(Method, URL, Headers, Body, RetryCnt-1);
        {RTT, {error, Reason}} ->
            {error, Reason};
        {RTT, {ok, Code, ResHeaders, Client}} ->
            {ok, ResBody} = hackney:body(Client),
            {ok, Code, ResBody}
    end.
