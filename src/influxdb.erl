%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(influxdb).
-behaviour(gen_server).

-include("influxdb.hrl").

%% API.
-export([ start_link/0
        , start_link/1
        ]).

%% gen_server.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ write/2
        , is_running/1
        ]).

-define(APP, influxdb).

-record(state, {
    write_protocol = ?DEFAULT_WRITE_PROTOCOL :: http | udp,

    batch_size = ?DEFAULT_BATCH_SIZE :: integer(),

    udp_socket = undefined :: gen_udp:socket() | undefined,

    udp_opts :: udp_opts(),

    http_opts :: http_opts()
}).

-import(proplists, [ get_value/2
                   , get_value/3]).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec(write(Pid, Points) -> ok | {erro, atom()}
    when Pid :: pid(),
         Points :: [Point],
         Point :: #{measurement := atom() | binary() | list(),
                    tags => map(),
                    fields := map(),
                    timestamp => integer()}).
write(Pid, Points) ->
    gen_server:cast(Pid, {write, Points}).

-spec(is_running(pid()) -> boolean()).
is_running(Pid) ->
    gen_server:call(Pid, is_running).

% %% gen_server.
init([Opts]) ->
    WriteProtocol = get_value(write_protocol, Opts, ?DEFAULT_WRITE_PROTOCOL),
    BatchSize = get_value(batch_size, Opts, ?DEFAULT_BATCH_SIZE),
    UDPOpts = merge_default_opts(get_value(udp_opts, Opts, []), ?DEFAULT_UDP_OPTS),
    HTTPOpts0 = merge_default_opts(get_value(http_opts, Opts, []), ?DEFAULT_HTTP_OPTS),
    URL = make_url(HTTPOpts0),
    HTTPOpts = lists:keyreplace(precision, 1, [{url, URL} | HTTPOpts0], {precision, to_string(get_value(precision, HTTPOpts0))}),
    State = #state{write_protocol = WriteProtocol,
                   batch_size = BatchSize,
                   http_opts = HTTPOpts,
                   udp_socket = undefined},
    case WriteProtocol of
        udp ->
            {ok, AddressFamily, IPAddress} = getaddr(get_value(host, UDPOpts)),
            {ok, Socket} = gen_udp:open(0, [binary, {active, false}, AddressFamily]),
            {ok, State#state{udp_opts = lists:keyreplace(host, 1, UDPOpts, {host, IPAddress}),
                             udp_socket = Socket}};
        http ->
            {ok, State}
    end.

handle_call(is_running, _From, State = #state{http_opts = HTTPOpts}) ->
    URL = string:join([get_value(url, HTTPOpts), "ping"], "/"),
    QueryParams = may_append_authentication_params([{"verbose", "true"}], HTTPOpts),    
    HTTPOptions = case get_value(https_enabled, HTTPOpts) of
                      false -> [];
                      true -> [{ssl, get_value(ssl, HTTPOpts, [])}]
                  end,
    NewURL = append_query_params_to_url(URL, QueryParams),
    case httpc:request(get, {NewURL, []}, HTTPOptions, []) of
        {ok, {{_, 200, _}, _, _}} -> {reply, true, State};
        _ -> {reply, false, State}
    end;

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

handle_cast({write, Points}, State = #state{write_protocol = WriteProtocol,
                                            batch_size = BatchSize,
                                            udp_socket = Socket,
                                            udp_opts = UDPOpts,
                                            http_opts = HTTPOpts}) ->
    NPoints = drain_points(BatchSize - length(Points), [Points]),
    case influxdb_line:encode(NPoints) of
        {error, Reason} ->
            logger:error("[InfluxDB] Encode ~p failed: ~p", [NPoints, Reason]);
        Data ->
            case WriteProtocol of
                udp ->
                    case gen_udp:send(Socket, get_value(host, UDPOpts), get_value(port, UDPOpts), Data) of
                        {error, Reason} ->
                            logger:error("[InfluxDB] Write ~p failed: ~p", [NPoints, Reason]);
                        _ ->
                            logger:debug("[InfluxDB] Write ~p successfully", [NPoints])
                    end;
                http ->
                    URL = string:join([get_value(url, HTTPOpts), "write"], "/"),
                    QueryParams = may_append_authentication_params([{"db", get_value(database, HTTPOpts)},
                                                                    {"precision", get_value(precision, HTTPOpts)}], HTTPOpts),                                              
                    HTTPOptions = case get_value(https_enabled, HTTPOpts) of
                                    false -> [{ssl, get_value(ssl, HTTPOpts)}];
                                    true -> []
                                end,
                    NewURL = append_query_params_to_url(URL, QueryParams),
                    case httpc:request(post, {NewURL, [], "text/plain", iolist_to_binary(Data)}, HTTPOptions, []) of
                        {ok, {{_, 204, _}, _, _}} ->
                            logger:debug("[InfluxDB] Write ~p successfully", [NPoints]);
                        {ok, {{_, StatusCode, ReasonPhrase}, _, Body}} ->
                            logger:error("[InfluxDB] Write ~p failed: ~p ~s, Details: ~s", [NPoints, StatusCode, ReasonPhrase, Body]);
                        {error, Reason} ->
                            logger:error("[InfluxDB] Write ~p failed: ~p", [NPoints, Reason])
                    end
            end
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

make_url(HTTPOpts) ->
    Host = binary_to_list(case list_to_binary(get_value(host, HTTPOpts)) of
                              <<"http://", Host0/binary>> -> Host0;
                              <<"https://", Host0/binary>> -> Host0;
                              Host0 -> Host0
                          end),
    Port = integer_to_list(get_value(port, HTTPOpts)),
    Scheme = case get_value(https_enabled, HTTPOpts) of
                true -> "https://";
                false -> "http://"
            end,
    Scheme ++ Host ++ ":" ++ Port.

merge_default_opts(Opts, Default) when is_list(Opts) ->
    merge_default_opts(maps:from_list(Opts), Default);
merge_default_opts(Opts, []) ->
    maps:to_list(Opts);
merge_default_opts(Opts, [{K, V} | More]) ->
    case maps:get(K, Opts, undefined) of
        undefined -> merge_default_opts(Opts#{K => V}, More);
        _ -> merge_default_opts(Opts, More)
    end.

getaddr(Host) ->
    case getaddr(Host, inet) of
        {ok, AddressFamily, IPAddress} ->
            {ok, AddressFamily, IPAddress};
        {error, _Reason} ->
            getaddr(Host, inet6)
    end.

getaddr(Host, AddressFamily)
  when AddressFamily =:= inet orelse AddressFamily =:= inet6 ->
    case inet:getaddr(Host, AddressFamily) of
        {ok, IPAddress} ->
            {ok, AddressFamily, IPAddress};
        {error, Reason} ->
            {error, Reason}
    end.

may_append_authentication_params(QueryParams0, AuthParams) ->
    QueryParams = [{"u", get_value(username, AuthParams, undefined)},
                   {"p", get_value(password, AuthParams, undefined)} | QueryParams0],
    lists:dropwhile(fun({_, K}) -> K =:= undefined end, QueryParams).

append_query_params_to_url(URL, QueryParams) ->
    do_append_query_params_to_url(URL ++ "?", QueryParams).

do_append_query_params_to_url(URL, [{K, V}]) ->
    URL ++ http_uri:encode(K) ++ "=" ++ http_uri:encode(V);
do_append_query_params_to_url(URL, [{K, V} | More]) ->
    NewURL = URL ++ http_uri:encode(K) ++ "=" ++ http_uri:encode(V) ++ "&",
    do_append_query_params_to_url(NewURL, More).

drain_points(0, Acc) ->
    lists:append(lists:reverse(Acc));
drain_points(Cnt, Acc) ->
    receive
        {'$gen_cast', {write, Points}} ->
            drain_points(Cnt - length(Points), [Points | Acc])
    after 0 ->
        lists:append(lists:reverse(Acc))
    end.

to_string(Bin) when is_binary(Bin) ->
    binary_to_list(Bin);
to_string(Str) when is_list(Str) ->
    Str;
to_string(Atom) when is_atom(Atom) ->
    atom_to_list(Atom).
