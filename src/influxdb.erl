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

-import(proplists, [get_value/3]).

%% API.
-export([start_link/0, start_link/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([ write/2
        , write/3
        , ping/1]).

-define(APP, influxdb).

-define(default_host, '127.0.0.1').
-define(default_port, 8089).

-record(state, {
    socket = undefined :: gen_udp:socket() | undefined,

    host = ?default_host :: inet:ip_address(),

    port = ?default_port :: inet:port_number(),

    set_timestamp = true :: boolean()
}).

%% API.

-spec start_link() -> {ok, pid()}.
start_link() ->
    start_link([]).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

-spec(write(Pid, Points) -> ok | {erro, atom()}
    when Pid :: pid(),
         Points :: [Point] | Point,
         Point :: #{measurement := atom() | binary() | list(),
                    tags => map(),
                    fields := map(),
                    timestamp => integer()}).
write(Pid, Points) ->
    write(Pid, Points, []).

-spec(write(Pid, Points, Options) -> ok | {erro, atom()}
    when Pid :: pid(),
         Points :: [Point] | Point,
         Point :: #{measurement := atom() | binary() | list(),
                    tags => map(),
                    fields := map(),
                    timestamp => integer()},
         Options :: [Option],
         Option :: {set_timestamp, boolean()}).
write(Pid, Points, Options) ->
    gen_server:call(Pid, {write, Points, Options}).

-spec(ping(pid()) -> ok | {erro, atom()}).
ping(Pid) ->
    gen_server:call(Pid, ping).

% %% gen_server.

init([Opts]) ->
    Host = proplists:get_value(host, Opts, ?default_host),
    case getaddr(Host) of
        {ok, Addr} ->
            {ok, Socket} = gen_udp:open(0, [binary, {active, false}, addr_family(Addr)]),
            {ok, #state{host = Addr,
                        socket = Socket,
                        port = proplists:get_value(port, Opts, ?default_port),
                        set_timestamp = proplists:get_value(set_timestamp, Opts, true)}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({write, Points, Options}, _From, State = #state{set_timestamp = SetTimestamp, 
                                                            socket = Socket,
                                                            host = Host,
                                                            port = Port}) ->
    LineOpts = [{set_timestamp, get_value(set_timestamp, Options, SetTimestamp)}],
    case influxdb_line:encode(Points, LineOpts) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        Data ->
            {reply, gen_udp:send(Socket, Host, Port, Data), State}
    end;

handle_call(ping, _From, State) ->
    case http_request(get, 
                      {url("http://127.0.0.1:8086/ping",[{"verbose", "true"}]), []}) of
        {ok, Headers, _Body} ->
            {reply, {ok, [{build_type, build_type(proplists:get_value("x-influxdb-build", Headers))},
                          {version, proplists:get_value("x-influxdb-version", Headers)}]}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(_Request, _From, State) ->
	{reply, ignored, State}.

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

getaddr(Host) ->
    case inet:getaddr(Host, inet) of
        {ok, Addr} -> {ok, Addr};
        {error, _Reason} -> inet:getaddr(Host, inet6)
    end.

addr_family({_, _, _, _}) -> inet;
addr_family({_, _, _, _, _, _, _, _}) -> inet6.

http_request(Method, Request) ->
    parse_response(httpc:request(Method, Request, [], [])).

url(Url, QueryParams) ->
    NewUrl = lists:foldl(fun({K, V}, Acc) ->
                    Acc ++ http_uri:encode(K) ++ "=" ++ http_uri:encode(V) ++ "&"
                end, Url ++ "?", QueryParams),
    case lists:last(NewUrl) of
        "&" -> lists:droplast(NewUrl);
        _ -> NewUrl
    end.

parse_response({ok, {{_, Code, _}, Headers, Body}}) ->
    parse_response({ok, Code, Headers, Body});
parse_response({ok, {Code, Body}}) ->
    parse_response({ok, Code, [], Body});
parse_response({ok, 200, Headers, Body}) ->
    {ok, Headers, Body};
parse_response({ok, 201, Headers, Body}) ->
    {ok, Headers, Body};
parse_response({ok, 204, Headers, _Body}) ->
    {ok, Headers, []};
parse_response({ok, Code, Headers, Body}) ->
    {error, {Code, Headers, Body}};
parse_response({error, Reason}) ->
    {error, Reason}.

build_type("OSS") ->
    open_source;
build_type(_) ->
    enterprise.