-module(gen_influxdb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [t_write].

init_per_suite(Config) ->
    application:ensure_all_started(gen_influxdb),
    Config.

end_per_suite(_Config) ->
    application:stop(gen_influxdb).

t_write(_) ->
    {ok, Pid} = gen_influxdb:start_link(),
    ok = gen_influxdb:write(Pid, [#{measurement => 'cpu',
                                    tags => #{host => 'serverA'},
                                    fields => #{value => 0.64}},
                                  #{measurement => 'temperature',
                                    tags => #{location => 'roomA'},
                                    fields => #{internal => 25, external => 37}}], [{set_timestamp, true}]).