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
        , update_precision/2
        , is_alive/1
        , is_alive/2
        , write/2
        , write/3
        , write_async/3
        , write_async/4
        , check_auth/1
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
                    ClientOptions = http_clients_options(Options),
                    {ok, maps:merge(ClientOptions, Client)};
                {error, {already_started, _}} ->
                    ClientOptions = http_clients_options(Options),
                    {error, {already_started, maps:merge(ClientOptions, Client)}};
                {error, Reason} ->
                    {error, Reason}
            end;
        udp ->
            case ecpool:start_sup_pool(Pool, influxdb_worker_udp, Options) of
                {ok, _} ->
                    {ok, Client};
                {error, {already_started, _}} ->
                    {error, {already_started, Client}};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

-spec update_precision(Client :: map(), precision()) -> Client :: map().
update_precision(#{protocol := http, opts := Opts0} = Client, Precision) ->
    Opts1 = [{precision, Precision} | proplists:delete(precision, Opts0)],
    Version = proplists:get_value(version, Opts1, v1),
    Client#{
        path => write_path(Version, Opts1),
        auth_path => auth_path(Version, Opts1),
        opts => Opts1
    };
update_precision(#{protocol := udp} = Client, _Precision) ->
    Client.

-spec(is_alive(Client :: map()) -> true | false).
is_alive(Client) ->
    is_alive(Client, false).

-spec(is_alive(Client :: map(), ReturnReason :: boolean()) -> true | false | {false, Reason :: term()}).
is_alive(#{protocol := Protocol} = Client, ReturnReason) ->
    case Protocol of
        http ->
            influxdb_http:is_alive(Client, ReturnReason);
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
    try
        case Protocol of
            http ->
                influxdb_http:write(Client, influxdb_line:encode(Points));
            udp ->
                influxdb_udp:write(Client, influxdb_line:encode(Points))
         end
    catch E:R:S ->
        logger:error("[InfluxDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
        {error, R}
    end.

-spec check_auth(Client :: map()) -> ok | {error, not_authorized} | {error, term()}.
check_auth(#{protocol := Protocol, auth_path := AuthPath} = Client0) ->
    FakePoint = #{fields => #{<<"check_auth">> => <<"">>}, measurement => <<"check_auth">>},
    Client = Client0#{path := AuthPath},
    Ret =
        try
            case Protocol of
                http ->
                    influxdb_http:write(Client, influxdb_line:encode(FakePoint));
                udp ->
                    influxdb_udp:write(Client, influxdb_line:encode(FakePoint))
            end
        catch E:R:S ->
            logger:error("[InfluxDB] Check auth ~0p failed: ~0p ~0p ~p", [FakePoint, E, R, S]),
            {error, {E, R}}
        end,
    case Ret of
        {_, {200, _, _}} -> ok;
        {_, {401, _, _}} -> {error, not_authorized};
        {error, Err} -> {error, Err};
        Err -> {error, Err}
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
    try
        case Protocol of
            http ->
                influxdb_http:write(Client, Key, influxdb_line:encode(Points));
            udp ->
                influxdb_udp:write(Client, Key, influxdb_line:encode(Points))
         end
    catch E:R:S ->
        logger:error("[InfluxDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
        {error, R}
    end.

-spec(write_async(Client, Points, {ReplayFun, Args}) -> {ok, pid()} | {error, term()}
when Client :: map(),
     Points :: [Point],
     Point :: #{measurement => atom() | binary() | list(),
                tags => map(),
                fields => map(),
                timestamp => integer()},
     ReplayFun :: function(),
     Args :: list()).
write_async(#{protocol := Protocol} = Client, Points, {ReplayFun, Args}) ->
    try
        case Protocol of
            http ->
                influxdb_http:write_async(Client, influxdb_line:encode(Points), {ReplayFun, Args});
            udp ->
                {error, udp_async_mode_not_supported}
         end
    catch E:R:S ->
        logger:error("[InfluxDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
        {error, R}
    end.

-spec(write_async(Client, Key, Points, {ReplayFun, Args}) -> ok | {error, term()}
when Client :: map(),
     Key :: any(),
     Points :: [Point],
     Point :: #{measurement => atom() | binary() | list(),
                tags => map(),
                fields => map(),
                timestamp => integer()},
     ReplayFun :: function(),
     Args :: list()).
write_async(#{protocol := Protocol} = Client, Key, Points, {ReplayFun, Args}) ->
    try
        case Protocol of
            http ->
                influxdb_http:write_async(Client, Key, influxdb_line:encode(Points), {ReplayFun, Args});
            udp ->
                {error, udp_async_mode_not_supported}
         end
    catch E:R:S ->
        logger:error("[InfluxDB] Encode ~0p failed: ~0p ~0p ~p", [Points, E, R, S]),
        {error, R}
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
%%% internal function
%%%-------------------------------------------------------------------

http_clients_options(Options) ->
    Version = proplists:get_value(version, Options, v1),
    Path = write_path(Version, Options),
    AuthPath = auth_path(Version, Options),
    Headers = header(Version, Options),
    PoolType = proplists:get_value(pool_type, Options, random),
    #{
        path => Path,
        auth_path => AuthPath,
        headers => Headers,
        version => Version,
        pool_type => PoolType,
        opts => Options
    }.

write_path(Version, Options) ->
    BasePath = write_path(Version),
    RawParams = [],
    path(BasePath, RawParams, Version, Options).

auth_path(Version, Options) ->
    BasePath = auth_path(Version),
    RawParams = [{"q", "show databases"}],
    path(BasePath, RawParams, Version, Options).

path(BasePath, RawParams, Version, Options) ->
    List0 = qs_list(Version),
    FoldlFun =
        fun({K1, K2}, Acc) ->
            case proplists:get_value(K2, Options) of
                undefined -> Acc;
                Val -> [{str(K1), str(Val)} | Acc]
            end
        end,
    List = lists:foldl(FoldlFun, RawParams, List0),
    case length(List) of
        0 ->
            BasePath;
        _ ->
            BasePath ++ "?" ++ uri_string:compose_query(List)
    end.

qs_list(v1) ->
    [
        {"db", database},
        {"u", username},
        {"p", password},
        {"precision", precision}
    ];
qs_list(v2) ->
    [
        {"org", org},
        {"bucket", bucket},
        {"precision", precision}
    ].

write_path(v1) -> "/write";
write_path(v2) -> "/api/v2/write".

header(v1, _) ->
    [{<<"Content-type">>, <<"text/plain; charset=utf-8">>}];
header(v2, Options) ->
    Token = proplists:get_value(token, Options, <<"">>),
    [{<<"Authorization">>, <<"Token ", Token/binary>>}] ++ header(v1, Options).

auth_path(_Version) -> "/query".

str(A) when is_atom(A) -> atom_to_list(A);
str(B) when is_binary(B) -> binary_to_list(B);
str(L) when is_list(L) -> L.
