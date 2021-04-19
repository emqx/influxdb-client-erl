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
-module(influxdb_worker_udp).

-behaviour(gen_server).

-behavihour(ecpool_worker).

-export([start_link/1]).

-export([write/2]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3]).

-export([connect/1]).

-define(SERVER, ?MODULE).

-record(state, {
    socket,
    host,
    port
}).

start_link(Options) ->
    gen_server:start_link(?MODULE, Options, []).

write(Pid, Data) ->
    gen_server:cast(Pid, {write, Data}).

init(Options) ->
    {ok, AddressFamily, IPAddress} = getaddr(proplists:get_value(host, Options)),
    {ok, Socket} = gen_udp:open(0, [binary, {active, false}, AddressFamily]),
    Port = proplists:get_value(port, Options),
    {ok, #state{socket = Socket, host = IPAddress, port = Port}}.

handle_call(_Request, _From, State) ->
    {reply, un_support_call, State}.

handle_cast({write, Data}, State = #state{socket = Socket, host = Host, port = Port}) ->
    gen_udp:send(Socket, Host, Port, Data),
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
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



%%%===================================================================
%%% ecpool callback
%%%===================================================================
connect(Option) ->
    start_link(Option).
