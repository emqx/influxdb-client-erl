-module(gen_influxdb).
-behaviour(gen_server).

-import(proplists, [get_value/3]).

%% API.
-export([start_link/0, start_link/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([write/2, write/3]).

-define(APP, gen_influxdb).

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

% %% gen_server.

init([Opts]) ->
    Host = proplists:get_value(host, Opts, ?default_host),
    case getaddr(Host) of
        {ok, Addr} ->
            {ok, Socket} = gen_udp:open(0, [binary, {active, false}, addr_family(Addr)]),
            {ok, #state{host = Addr,
                        socket = Socket,
                        port = proplists:get_value(port, Opts, ?default_port)}};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({write, Points, Options}, _From, State = #state{set_timestamp = SetTimestamp, 
                                                            socket = Socket,
                                                            host = Host,
                                                            port = Port}) ->
    LineOpts = [{set_timestamp, get_value(set_timestamp, Options, SetTimestamp)}],
    case gen_influxdb_line:encode(Points, LineOpts) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        Data ->
            {reply, gen_udp:send(Socket, Host, Port, Data), State}
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