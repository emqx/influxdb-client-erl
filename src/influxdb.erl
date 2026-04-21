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
check_auth(#{protocol := Protocol} = Client) ->
    case Protocol of
        http ->
            influxdb_http:check_auth(Client);
        udp ->
            ok
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

auth_path(v1, Options) ->
    BasePath = "/query",
    %% No query parameter — we only need credential validation.
    %% InfluxDB returns 401 for bad credentials, 400 for missing "q" param
    %% (which means credentials are valid).
    RawParams = [],
    path(BasePath, RawParams, v1, Options);
auth_path(v2, _Options) ->
    %% v2/v3 check_auth uses dedicated endpoints in influxdb_http,
    %% no auth_path needed.
    undefined;
auth_path(v3, _Options) ->
    undefined.

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
        {"precision", precision}
    ];
qs_list(v2) ->
    [
        {"org", org},
        {"bucket", bucket},
        {"precision", precision}
    ];
qs_list(v3) ->
    [
        {"db", database}
    ].

write_path(v1) -> "/write";
write_path(v2) -> "/api/v2/write";
write_path(v3) -> "/api/v3/write_lp".

header(v1, Options) ->
    maybe_add_basic_auth_header(
        [{<<"Content-type">>, <<"text/plain; charset=utf-8">>}],
        Options
    );
header(v2, Options) ->
    Token = proplists:get_value(token, Options, <<"">>),
    [{<<"Authorization">>, <<"Token ", Token/binary>>}, {<<"Content-type">>, <<"text/plain; charset=utf-8">>}];
header(v3, Options) ->
    Token = proplists:get_value(token, Options, <<"">>),
    [{<<"Authorization">>, <<"Bearer ", Token/binary>>}, {<<"Content-type">>, <<"text/plain; charset=utf-8">>}].


str(A) when is_atom(A) -> atom_to_list(A);
str(B) when is_binary(B) -> binary_to_list(B);
str(L) when is_list(L) -> L.

maybe_add_basic_auth_header(Headers, Options) ->
    case basic_auth_value(Options) of
        undefined -> Headers;
        Authorization -> [{<<"Authorization">>, Authorization} | Headers]
    end.

basic_auth_value(Options) ->
    case {proplists:get_value(username, Options), proplists:get_value(password, Options)} of
        {undefined, undefined} ->
            undefined;
        {Username, Password} ->
            Encoded = base64:encode(
                iolist_to_binary([to_binary_or_empty(Username), <<":">>, to_binary_or_empty(Password)])
            ),
            <<"Basic ", Encoded/binary>>
    end.

to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> unicode:characters_to_binary(Value);
to_binary(Value) when is_atom(Value) -> atom_to_binary(Value, utf8).

to_binary_or_empty(undefined) -> <<>>;
to_binary_or_empty(Value) -> to_binary(Value).

%%===================================================================
%% eunit tests
%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

auth_path_v1_no_show_databases_test() ->
    Options = [{database, "mydb"}, {username, "user"}, {password, "pass"}],
    Path = auth_path(v1, Options),
    %% auth_path must NOT contain "show databases" or any "q=" parameter
    ?assertNotEqual(nomatch, string:find(Path, "/query")),
    ?assertEqual(nomatch, string:find(Path, "show")),
    ?assertEqual(nomatch, string:find(Path, "SHOW")),
    ?assertEqual(nomatch, string:find(Path, "q=")),
    %% and must not leak credentials into the query string
    ?assertEqual(nomatch, string:find(Path, "u=user")),
    ?assertEqual(nomatch, string:find(Path, "p=pass")).

auth_path_v2_undefined_test() ->
    Options = [{token, <<"mytoken">>}, {org, "myorg"}, {bucket, "mybucket"}],
    ?assertEqual(undefined, auth_path(v2, Options)).

auth_path_v3_undefined_test() ->
    Options = [{token, <<"mytoken">>}, {database, "mydb"}],
    ?assertEqual(undefined, auth_path(v3, Options)).

http_clients_options_v1_no_show_databases_test() ->
    Options = [{version, v1}, {database, "mydb"}, {username, "user"}, {password, "pass"}],
    #{auth_path := AuthPath} = http_clients_options(Options),
    ?assertNotEqual(nomatch, string:find(AuthPath, "/query")),
    ?assertEqual(nomatch, string:find(AuthPath, "show")),
    ?assertEqual(nomatch, string:find(AuthPath, "q=")).

v1_paths_do_not_include_ping_auth_params_test() ->
    Options = [{version, v1}, {database, "mydb"}, {username, "user"}, {password, "pass"}],
    #{path := WritePath, auth_path := AuthPath} = http_clients_options(Options),
    ?assertNotEqual(nomatch, string:find(WritePath, "/write")),
    ?assertNotEqual(nomatch, string:find(WritePath, "db=mydb")),
    ?assertEqual(nomatch, string:find(WritePath, "u=")),
    ?assertEqual(nomatch, string:find(WritePath, "p=")),
    ?assertNotEqual(nomatch, string:find(AuthPath, "/query")),
    ?assertEqual(nomatch, string:find(AuthPath, "u=")),
    ?assertEqual(nomatch, string:find(AuthPath, "p=")).

v1_header_uses_basic_auth_test() ->
    Options = [{username, <<"user">>}, {password, <<"pass">>}],
    Headers = header(v1, Options),
    ?assert(lists:member(
        {<<"Authorization">>, <<"Basic dXNlcjpwYXNz">>},
        Headers
    )).

v1_header_supports_username_without_password_test() ->
    Headers = header(v1, [{username, <<"user">>}]),
    ?assert(lists:member(
        {<<"Authorization">>, <<"Basic dXNlcjo=">>},
        Headers
    )).

v1_header_supports_password_without_username_test() ->
    Headers = header(v1, [{password, <<"pass">>}]),
    ?assert(lists:member(
        {<<"Authorization">>, <<"Basic OnBhc3M=">>},
        Headers
    )).

v2_header_does_not_include_basic_auth_test() ->
    Options = [{token, <<"tok">>}, {username, <<"user">>}, {password, <<"pass">>}],
    Headers = header(v2, Options),
    AuthorizationHeaders = [Value || {Key, Value} <- Headers, Key =:= <<"Authorization">>],
    ?assertEqual([<<"Token tok">>], AuthorizationHeaders).

v3_header_does_not_include_basic_auth_test() ->
    Options = [{token, <<"tok">>}, {username, <<"user">>}, {password, <<"pass">>}],
    Headers = header(v3, Options),
    AuthorizationHeaders = [Value || {Key, Value} <- Headers, Key =:= <<"Authorization">>],
    ?assertEqual([<<"Bearer tok">>], AuthorizationHeaders).

v1_is_alive_with_ping_auth_enabled_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = ping_auth_server_start(<<"Authorization: Basic dXNlcjpwYXNz\r\n">>),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("ping_auth_good")}
        , {pool_size, 1}
        , {pool_type, random}
        , {username, <<"user">>}
        , {password, <<"pass">>}
        , {database, <<"mydb">>}
        , {ping_with_auth, true}
        , {version, v1}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertEqual(true, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
        end.

v1_is_alive_accepts_verbose_ping_auth_response_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = ping_auth_server_start(<<"Authorization: Basic dXNlcjpwYXNz\r\n">>, <<"HTTP/1.1 200 OK\r\n">>),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("ping_auth_verbose")}
        , {pool_size, 1}
        , {pool_type, random}
        , {username, <<"user">>}
        , {password, <<"pass">>}
        , {database, <<"mydb">>}
        , {ping_with_auth, true}
        , {version, v1}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertEqual(true, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
    end.

v1_is_alive_defaults_to_legacy_ping_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = auth_header_ping_server_start(undefined),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("ping_legacy")}
        , {pool_size, 1}
        , {pool_type, random}
        , {username, <<"user">>}
        , {password, <<"pass">>}
        , {database, <<"mydb">>}
        , {version, v1}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertEqual(true, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
    end.

v1_is_alive_rejects_bad_ping_auth_when_enabled_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = ping_auth_server_start(<<"Authorization: Basic dXNlcjpwYXNz\r\n">>),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("ping_auth_bad")}
        , {pool_size, 1}
        , {pool_type, random}
        , {username, <<"user">>}
        , {password, <<"wrong">>}
        , {database, <<"mydb">>}
        , {ping_with_auth, true}
        , {version, v1}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertMatch({false, _}, influxdb:is_alive(Client, true)),
        ?assertEqual(false, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
    end.

v2_is_alive_defaults_to_ping_without_auth_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = auth_header_ping_server_start(undefined),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("v2_ping_no_header")}
        , {pool_size, 1}
        , {pool_type, random}
        , {token, <<"tok">>}
        , {version, v2}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertEqual(true, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
    end.

v2_is_alive_with_ping_auth_enabled_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = auth_header_ping_server_start(<<"Authorization: Token tok\r\n">>),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("v2_ping_header")}
        , {pool_size, 1}
        , {pool_type, random}
        , {token, <<"tok">>}
        , {ping_with_auth, true}
        , {version, v2}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertEqual(true, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
    end.

v3_is_alive_defaults_to_ping_without_auth_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = auth_header_ping_server_start(undefined),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("v3_ping_no_header")}
        , {pool_size, 1}
        , {pool_type, random}
        , {token, <<"tok">>}
        , {database, <<"mydb">>}
        , {version, v3}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertEqual(true, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
    end.

v3_is_alive_with_ping_auth_enabled_test() ->
    application:ensure_all_started(influxdb),
    ListenSocket = auth_header_ping_server_start(<<"Authorization: Bearer tok\r\n">>),
    {ok, {_Addr, Port}} = inet:sockname(ListenSocket),
    Options =
        [ {host, "127.0.0.1"}
        , {port, Port}
        , {protocol, http}
        , {https_enabled, false}
        , {pool, pool_name("v3_ping_header")}
        , {pool_size, 1}
        , {pool_type, random}
        , {token, <<"tok">>}
        , {database, <<"mydb">>}
        , {ping_with_auth, true}
        , {version, v3}
        ],
    {ok, Client} = influxdb:start_client(Options),
    try
        ?assertEqual(true, influxdb:is_alive(Client))
    after
        ok = influxdb:stop_client(Client)
    end.

http_clients_options_v2_auth_path_undefined_test() ->
    Options = [{version, v2}, {token, <<"tok">>}, {org, "org"}, {bucket, "bkt"}],
    #{auth_path := AuthPath} = http_clients_options(Options),
    ?assertEqual(undefined, AuthPath).

http_clients_options_v3_auth_path_undefined_test() ->
    Options = [{version, v3}, {token, <<"tok">>}, {database, "mydb"}],
    #{auth_path := AuthPath} = http_clients_options(Options),
    ?assertEqual(undefined, AuthPath).

ping_auth_server_start(ExpectedAuthorization) ->
    ping_auth_server_start(ExpectedAuthorization, <<"HTTP/1.1 204 No Content\r\n">>).

ping_auth_server_start(ExpectedAuthorization, SuccessStatusLine) ->
    {ok, ListenSocket} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    spawn_link(fun() -> ping_auth_server_serve(ListenSocket, ExpectedAuthorization, SuccessStatusLine) end),
    ListenSocket.

auth_header_ping_server_start(ExpectedHeader) ->
    {ok, ListenSocket} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    spawn_link(fun() -> auth_header_ping_server_serve(ListenSocket, ExpectedHeader) end),
    ListenSocket.

ping_auth_server_serve(ListenSocket, ExpectedAuthorization, SuccessStatusLine) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    {ok, Request} = gen_tcp:recv(Socket, 0, 5000),
    StatusLine =
        case ping_auth_request_result(Request, ExpectedAuthorization) of
            true -> SuccessStatusLine;
            false -> <<"HTTP/1.1 401 Unauthorized\r\n">>
        end,
    _ = gen_tcp:send(Socket, [StatusLine, <<"Content-Length: 0\r\nConnection: close\r\n\r\n">>]),
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(ListenSocket).

auth_header_ping_server_serve(ListenSocket, ExpectedHeader) ->
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    {ok, Request} = gen_tcp:recv(Socket, 0, 5000),
    StatusLine =
        case auth_header_ping_request_result(Request, ExpectedHeader) of
            true -> <<"HTTP/1.1 204 No Content\r\n">>;
            false -> <<"HTTP/1.1 401 Unauthorized\r\n">>
        end,
    _ = gen_tcp:send(Socket, [StatusLine, <<"Content-Length: 0\r\nConnection: close\r\n\r\n">>]),
    ok = gen_tcp:close(Socket),
    ok = gen_tcp:close(ListenSocket).

auth_header_ping_request_result(Request, undefined) ->
    contains(Request, <<"GET /ping HTTP/">>) andalso
        not contains_ci(Request, <<"authorization: ">>);
auth_header_ping_request_result(Request, ExpectedHeader) ->
    contains(Request, <<"GET /ping HTTP/">>) andalso contains_ci(Request, ExpectedHeader).

ping_auth_request_result(Request, undefined) ->
    contains(Request, <<"GET /ping HTTP/">>);
ping_auth_request_result(Request, ExpectedAuthorization) ->
    contains(Request, <<"GET /ping HTTP/">>) andalso
        contains_ci(Request, ExpectedAuthorization) andalso
        not contains(Request, <<"GET /ping?">>) andalso
        not contains_ci(Request, <<"u=">>) andalso
        not contains_ci(Request, <<"p=">>).

contains(Binary, Pattern) ->
    binary:match(Binary, Pattern) =/= nomatch.

contains_ci(Binary, Pattern) ->
    LowerBinary = string:lowercase(binary_to_list(Binary)),
    LowerPattern = string:lowercase(binary_to_list(Pattern)),
    string:find(LowerBinary, LowerPattern) =/= nomatch.

pool_name(Prefix) ->
    list_to_binary(Prefix ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))).

-endif.
