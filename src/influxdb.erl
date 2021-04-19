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
        , write_sync/2
        , is_running/1
        ]).


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

-spec(write(Pid, Points) -> ok | {error, atom()}
    when Pid :: pid(),
         Points :: [Point],
         Point :: #{measurement := atom() | binary() | list(),
                    tags => map(),
                    fields := map(),
                    timestamp => integer()}).
write(Pid, Points) ->
    write(Pid, Points, [{schedule, async}]).

-spec(write_sync(Pid, Points) -> ok | {error, term()}
when Pid :: pid(),
     Points :: [Point],
     Point :: #{measurement := atom() | binary() | list(),
                tags => map(),
                fields := map(),
                timestamp => integer()}).
write_sync(Pid, Points) ->
    write(Pid, Points, [{schedule, sync}]).

-spec(write(Pid, Points, Opts) -> ok | {error, atom()}
    when Pid :: pid(),
         Points :: [Point],
         Opts :: [{schedule, async | sync}],
         Point :: #{measurement := atom() | binary() | list(),
                    tags => map(),
                    fields := map(),
                    timestamp => integer()}).
write(Pid, Points, Opts) ->
    case proplists:get_value(schedule, Opts, sync) of
        sync -> 
            gen_server:call(Pid, {write, Points});
        aync ->         
            gen_server:cast(Pid, {write, Points})
    end.

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
	        application:ensure_all_started(ehttpc),
            ehttpc_sup:start_pool(?APP, HTTPOpts),
            {ok, State#state{http_opts = HTTPOpts ++ [{path, make_path(HTTPOpts)}]}}
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

handle_call({write, Points}, _From, State) ->
    case do_write(Points, State) of
        {ok, NewState} -> 
            {reply, ok, NewState};
        {fail, Reason, NewState} -> 
            {reply, {error, Reason}, NewState}
    end;

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({write, Points}, State) ->
    case do_write(Points, State) of
        {ok, NewState} -> 
            {noreply, NewState};
        {fail, _Reason, NewSate} -> 
            {noreply, NewSate}
    end;

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
    ehttpc_sup:stop_pool(?APP).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
do_write(Points, State = #state{write_protocol = WriteProtocol,
                                batch_size = BatchSize,
                                udp_socket = Socket,
                                udp_opts = UDPOpts,
                                http_opts = HTTPOpts})->
    NPoints = drain_points(BatchSize - length(Points), [Points]),
    case influxdb_line:encode(NPoints) of
        {error, Reason} ->
            logger:error("[InfluxDB] Encode ~p failed: ~p", [NPoints, Reason]),
            {fail, Reason, State};
        Data ->
            case WriteProtocol of
                udp ->
                    case gen_udp:send(Socket, get_value(host, UDPOpts), get_value(port, UDPOpts), Data) of
                        {error, Reason} ->
                            logger:error("[InfluxDB] Write ~p failed: ~p", [NPoints, Reason]),
                            {fail, Reason, State};
                        _ ->
                            logger:debug("[InfluxDB] Write ~p successfully", [NPoints]),
                            {ok, State}
                    end;
                http ->
                    case ehttpc_write(Data, HTTPOpts) of
                        ok ->
                            logger:debug("[InfluxDB] Write ~p successfully", [NPoints]),
                            {ok, State}; 
                        {fail, Reason} ->
                            logger:error("[InfluxDB] Write ~p failed: ~p", [NPoints, Reason]),
                            {fail, Reason}
                    end
            end
    end.

ehttpc_write(Data, HTTPOpts)->
    Path = proplists:get_value(path, HTTPOpts),
    Headers = [{<<"content-type">>, <<"text/plain">>}],
    case ehttpc:request(ehttpc_pool:pick_worker(?APP), post, {Path, Headers, Data}) of
        {ok, 204, _} ->
            ok;
        {ok, 204, _, _} ->
            ok;
        {ok, StatusCode, Reason} ->
            {fail, {StatusCode, Reason}};
        {ok, StatusCode, Reason, Body} ->
            {fail, {StatusCode, Reason, Body}};
        {error, Reason} ->
            {fail, Reason}
    end.

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

make_path(HTTPOpts) ->
    Database = proplists:get_value(database, HTTPOpts),
    Username = proplists:get_value(username, HTTPOpts),
    Password = proplists:get_value(password, HTTPOpts),
    Precision = proplists:get_value(precision, HTTPOpts),
    List0 = [{<<"db">>, Database}, {<<"u">>, Username}, {<<"p">>, Password}, {<<"precision">>, Precision}],
    Filter = fun(Arg) -> 
                case Arg of
                    {_, undefined} -> false;
                    {_, _} -> true
                end
            end,
    List = lists:filter(Filter, List0),
    case length(List) of
        0 -> 
            "/write";
        _ -> 
            "/write?" ++ uri_string:compose_query(List)
    end.

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
