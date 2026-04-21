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

-export([ is_alive/2
        , check_auth/1
        , write/2
        , write/3
        , write_async/3
        , write_async/4]).

-ifdef(TEST).
-export([ping_auth_params/1]).
-endif.

is_alive(Client = #{version := Version}, ReturnReason) ->
    is_alive(Version, Client, ReturnReason);
is_alive(Client, ReturnReason) ->
    is_alive(v1, Client, ReturnReason).

is_alive(V, Client, ReturnReason) when V == v2; V == v3 ->
    Path = "/ping",
    Headers = ping_headers(Client),
    try
        Worker = pick_worker(Client, ignore),
        case ehttpc:request(Worker, get, {Path, Headers}) of
            {ok, 200, _} ->
                true;
            {ok, 200, _, _} ->
                true;
            {ok, 204, _} ->
                true;
            {ok, 204, _, _} ->
                true;
            Return ->
                maybe_return_reason(Return, ReturnReason)
        end
    catch E:R:S ->
        log_or_return_reason(#{exception => E, reason => R, stacktrace => S},
                             ReturnReason)
    end;
is_alive(v1, Client, ReturnReason) ->
    Path = v1_ping_path(Client),
    Headers = [{<<"verbose">>, <<"true">>}],
    try
        Worker = pick_worker(Client, ignore),
        case ehttpc:request(Worker, get, {Path, Headers}) of
            {ok, 204, _} ->
                true;
            {ok, 204, _, _} ->
                true;
            Return ->
                maybe_return_reason(Return, ReturnReason)
        end
    catch E:R:S ->
        log_or_return_reason(#{exception => E, reason => R, stacktrace => S},
                             ReturnReason)
    end.

%% @doc Check authentication against the InfluxDB server.
%% For v2, uses GET /api/v2/buckets with the Authorization header.
%% For v3, uses POST /api/v3/query_sql with the Authorization header (empty body).
%% For v1, uses GET /query (without "q" param) with credentials in query string;
%%   returns ok on 200 or 400 (missing "q" means auth passed), error on 401.
-spec check_auth(Client :: map()) -> ok | {error, not_authorized} | {error, term()}.
check_auth(#{version := v2} = Client) ->
    check_auth_v2(Client);
check_auth(#{version := v3} = Client) ->
    check_auth_v3(Client);
check_auth(#{version := v1} = Client) ->
    check_auth_v1(Client);
check_auth(Client) ->
    %% default to v1
    check_auth_v1(Client).

write(Client = #{path := Path, headers := Headers}, Data) ->
    Request = {Path, Headers, Data},
    do_write(pick_worker(Client, ignore), Request).

write(Client = #{path := Path, headers := Headers}, Key, Data) ->
    Request = {Path, Headers, Data},
    do_write(pick_worker(Client, Key), Request).

write_async(Client = #{path := Path, headers := Headers}, Data, ReplayFunAndArgs) ->
    Request = {Path, Headers, Data},
    do_aysnc_write(pick_worker(Client, ignore), Request, ReplayFunAndArgs).

write_async(Client = #{path := Path, headers := Headers}, Key, Data, ReplayFunAndArgs) ->
    Request = {Path, Headers, Data},
    do_aysnc_write(pick_worker(Client, Key), Request, ReplayFunAndArgs).

%%==============================================================================
%% Internal funcs

check_auth_v2(#{headers := Headers} = Client) ->
    Path = "/api/v2/buckets?limit=1",
    try
        Worker = pick_worker(Client, ignore),
        case ehttpc:request(Worker, get, {Path, Headers}) of
            {ok, Code, _} when Code >= 200, Code < 300 ->
                ok;
            {ok, Code, _, _} when Code >= 200, Code < 300 ->
                ok;
            {ok, 401, _} ->
                {error, not_authorized};
            {ok, 401, _, _} ->
                {error, not_authorized};
            {ok, Code, _} ->
                {error, {unexpected_status, Code}};
            {ok, Code, _, Body} ->
                {error, {unexpected_status, Code, Body}};
            {error, Reason} ->
                {error, Reason}
        end
    catch E:R:S ->
        logger:error("[InfluxDB] check_auth v2 exception: ~0p ~0p ~0p", [E, R, S]),
        {error, {E, R}}
    end.

check_auth_v3(#{headers := Headers} = Client) ->
    %% POST /api/v3/query_sql with empty body.
    %% InfluxDB v3 authenticates first, then validates the request:
    %%   - 401: token is invalid
    %%   - 400/422: token is valid but request is malformed (no query body)
    %%   - 200: token is valid (unlikely for empty body, but accept it)
    Path = "/api/v3/query_sql",
    try
        Worker = pick_worker(Client, ignore),
        case ehttpc:request(Worker, post, {Path, Headers, <<>>}) of
            {ok, Code, _} when Code >= 200, Code < 300 ->
                ok;
            {ok, Code, _, _} when Code >= 200, Code < 300 ->
                ok;
            {ok, 400, _} ->
                ok;
            {ok, 400, _, _} ->
                ok;
            {ok, 422, _} ->
                ok;
            {ok, 422, _, _} ->
                ok;
            {ok, 401, _} ->
                {error, not_authorized};
            {ok, 401, _, _} ->
                {error, not_authorized};
            {ok, Code, _} ->
                {error, {unexpected_status, Code}};
            {ok, Code, _, Body} ->
                {error, {unexpected_status, Code, Body}};
            {error, Reason} ->
                {error, Reason}
        end
    catch E:R:S ->
        logger:error("[InfluxDB] check_auth v3 exception: ~0p ~0p ~0p", [E, R, S]),
        {error, {E, R}}
    end.

check_auth_v1(#{auth_path := AuthPath, headers := Headers} = Client) ->
    %% Send GET /query with credentials but without "q" parameter.
    %% InfluxDB v1 authenticates the request first, then checks for "q":
    %%   - 401: credentials are invalid
    %%   - 400 (missing "q"): credentials are valid
    %%   - 200: credentials are valid (auth disabled on server)
    try
        Worker = pick_worker(Client, ignore),
        case ehttpc:request(Worker, get, {AuthPath, Headers}) of
            {ok, 200, _} ->
                ok;
            {ok, 200, _, _} ->
                ok;
            {ok, 400, _, _} ->
                %% "missing required parameter q" — auth passed
                ok;
            {ok, 400, _} ->
                ok;
            {ok, 401, _} ->
                {error, not_authorized};
            {ok, 401, _, _} ->
                {error, not_authorized};
            {ok, Code, _} ->
                {error, {unexpected_status, Code}};
            {ok, Code, _, Body} ->
                {error, {unexpected_status, Code, Body}};
            {error, Reason} ->
                {error, Reason}
        end
    catch E:R:S ->
        logger:error("[InfluxDB] check_auth v1 exception: ~0p ~0p ~0p", [E, R, S]),
        {error, {E, R}}
    end;
check_auth_v1(_Client) ->
    %% No auth_path available, skip auth check
    ok.

maybe_return_reason({ok, ReturnCode, _}, true) ->
    {false, ReturnCode};
maybe_return_reason({ok, ReturnCode, _, Body}, true) ->
    {false, {ReturnCode, Body}};
maybe_return_reason({error, Reason}, true) ->
    {false, Reason};
maybe_return_reason(_, _) ->
    false.

log_or_return_reason(#{} = Reason, true) ->
    {false, Reason};
log_or_return_reason(#{exception := E, reason := R, stacktrace := S}, _) ->
    logger:error("[InfluxDB] is_alive exception: ~0p ~0p ~0p", [E, R, S]),
    false.

do_write(Worker, {_Path, _Headers, _Data} = Request) ->
    try ehttpc:request(Worker, post, Request) of
        {ok, 204, _} ->
            ok;
        {ok, 204, _, _} ->
            ok;
        {ok, StatusCode, Reason} ->
            {error, {StatusCode, Reason}};
        {ok, StatusCode, Reason, Body} ->
            {error, {StatusCode, Reason, Body}};
        Error ->
            {error, Error}
    catch E:R:S ->
        logger:error("[InfluxDB] http write fail: ~0p ~0p ~0p", [E, R, S]),
        {error, {E, R}}
    end.

do_aysnc_write(Worker, Request, ReplayFunAndArgs) ->
    ok = ehttpc:request_async(Worker, post, Request, 5000, ReplayFunAndArgs),
    {ok, Worker}.

v1_ping_path(#{opts := Options}) ->
    Params = ping_auth_params(Options),
    case Params of
        [] -> "/ping";
        _ -> "/ping?" ++ Params
    end;
v1_ping_path(_Client) ->
    "/ping".

ping_auth_params(Options) ->
    case ping_query_auth_enabled(Options) of
        true ->
            uri_string:compose_query(
                lists:reverse(
                    add_query_param(password, "p",
                        add_query_param(username, "u", [{"verbose", "true"}], Options),
                    Options)
                )
            );
        false ->
            []
    end.

ping_query_auth_enabled(Options) ->
    proplists:get_value(version, Options, v1) =:= v1 andalso
        ping_with_auth_enabled(Options).

ping_with_auth_enabled(Options) ->
    proplists:get_value(ping_with_auth, Options, false) =:= true.

ping_headers(#{headers := Headers, opts := Options}) ->
    case ping_with_auth_enabled(Options) of
        true ->
            Headers;
        false ->
            lists:keydelete(<<"Authorization">>, 1, Headers)
    end;
ping_headers(#{headers := Headers}) ->
    Headers.

add_query_param(Key, Name, Acc, Options) ->
    case proplists:get_value(Key, Options) of
        undefined -> Acc;
        Val when is_binary(Val) -> [{Name, binary_to_list(Val)} | Acc];
        Val when is_list(Val) -> [{Name, Val} | Acc];
        Val when is_atom(Val) -> [{Name, atom_to_list(Val)} | Acc]
    end.

pick_worker(#{pool := Pool, pool_type := hash}, Key) ->
    ehttpc_pool:pick_worker(Pool, Key);
pick_worker(#{pool := Pool}, _Key) ->
    ehttpc_pool:pick_worker(Pool).
