-module(gen_influxdb_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
	gen_influxdb_sup:start_link().

stop(_State) ->
	ok.
