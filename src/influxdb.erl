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
-module(influxdb).
-include("influxdb.hrl").

-export([ start_client/1
        , is_alive/1
        , write/2
        , write/3
        , stop_client/1]).

-spec(start_client(list()) -> {ok, Client :: map()} | {error, {already_started, Client :: map()}} | {error, Reason :: term()}).
start_client(Options0) ->
    Pool = proplists:get_value(pool, Options0),
    Protocol = proplists:get_value(protocol, Options0, http),
    Client = #{
        pool => Pool,
        protocol => Protocol
    },
    Options = lists:keydelete(protocol, 1, lists:keydelete(pool, 1, Options0)), 
    case Protocol of
        http ->
            case ehttpc_sup:start_pool(Pool, Options) of
                {ok, _} ->
                    Path = make_path(Options),
                    {ok, maps:put(path, Path, Client)};
                {error, {already_started, _}} ->
                    Path = make_path(Options),
                    {error, {already_started, maps:put(path, Path, Client)}};
                {error, Reason} ->
                    {error, Reason};
                Error ->
                    {error, Error}
            end;
        udp ->
            case ecpool:start_sup_pool(Pool, influxdb_worker_udp, Options) of
                {ok, _} ->
                    {ok, Client};
                {error, {already_started, _}} ->
                    {error, {already_started, Client}};
                {error, Reason} ->
                    {error, Reason};
                Error -> 
                    {error, Error}
            end
    end.

-spec(is_alive(Client :: map()) -> true | false).
is_alive(#{protocol := Protocol} = Client) ->
    case Protocol of
        http ->
            influxdb_http:is_alive(Client);
        udp ->
            true
    end.

-spec(write(Client, Points) -> ok | {error, term()}
when Client :: map(),
     Points :: [Point],
     Point :: #{measurement => atom() | binary() | list(),
                tags => map(),
                fields => map(),
                timestamp => integer()}).
write(#{protocol := Protocol} = Client, Points) ->
    case influxdb_line:encode(Points) of
        {error, Reason} ->
            logger:error("[InfluxDB] Encode ~0p failed: ~0p", [Points, Reason]),
            {error, Reason};
        Data ->
            case Protocol of
                http -> 
                    influxdb_http:write(Client, Data);
                udp ->
                    influxdb_udp:write(Client, Data)
             end
    end.

-spec(write(Client, Key, Points) -> ok | {error, term()}
when Client :: map(),
     Key :: any(),
     Points :: [Point],
     Point :: #{measurement => atom() | binary() | list(),
                tags => map(),
                fields => map(),
                timestamp => integer()}).
write(#{protocol := Protocol} = Client, Key, Points) ->
    case influxdb_line:encode(Points) of
        {error, Reason} ->
            logger:error("[InfluxDB] Encode ~0p failed: ~0p", [Points, Reason]),
            {error, Reason};
        Data ->
            case Protocol of
                http -> 
                    influxdb_http:write(Client, Key, Data);
                udp ->
                    influxdb_udp:write(Client, Key, Data)
             end
    end.

-spec(stop_client(Client :: map()) -> ok | term()).
stop_client(#{pool := Pool, protocol := Protocol}) ->
    case Protocol of
        http ->
            ehttpc_sup:stop_pool(Pool);
        udp ->
            ecpool:stop_sup_pool(Pool)
    end.

%%%-------------------------------------------------------------------
%%% internale function
%%%-------------------------------------------------------------------
make_path(InfluxdbConf) ->
    Host = proplists:get_value(host, InfluxdbConf),
    Port = proplists:get_value(port, InfluxdbConf),

    Url = case proplists:get_value(https_enabled, InfluxdbConf) of
        true ->  lists:concat(["https://", Host, ":", Port]);
        false -> lists:concat(["http://", Host, ":", Port])
    end,
    List0 = [{"db", database},
            {"u", username},
            {"p", password},
            {"precision", precision}],
    FoldlFun = fun({K1, K2}, Acc) ->
                    case proplists:get_value(K2, InfluxdbConf) of
                        undefined -> Acc;
                        Val -> [{K1, Val}| Acc]
                    end
                end,
    List = lists:foldl(FoldlFun, [], List0),
    case length(List) of
        0 ->
        Url ++ "/write";
        _ ->
        Url ++ "/write?" ++ uri_string:compose_query(List)
    end.
