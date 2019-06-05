-module(gen_influxdb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [ t_encode_line
         , t_write
         ].

init_per_suite(Config) ->
    application:ensure_all_started(gen_influxdb),
    Config.

end_per_suite(_Config) ->
    application:stop(gen_influxdb).

t_encode_line(_) ->
    ?assertEqual(<<"cpu value=0.64\n">>,
                 iolist_to_binary(gen_influxdb_line:encode(#{measurement => 'cpu', fields => #{value => 0.64}}))),
    ?assertEqual(<<"cpu,host=serverA,region=us_west value=0.64\n">>,
                 iolist_to_binary(gen_influxdb_line:encode(#{measurement => 'cpu', fields => #{value => 0.64}, tags => #{host => 'serverA', region => 'us_west'}}))),

    ?assertEqual(<<"cpu,host=serverA,region=us_west value=1i 100\n">>,
                 iolist_to_binary(gen_influxdb_line:encode(#{measurement => 'cpu', fields => #{value => 1}, tags => #{host => 'serverA', region => 'us_west'}, timestamp => 100}))),

    %% special characters
    ?assertEqual(<<"cp\\ u v\\ a\\=l\\,ue=0.64\n">>,
                 iolist_to_binary(gen_influxdb_line:encode(#{measurement => 'cp u', fields => #{"v a=l,ue" => 0.64}}))),

    ?assertEqual({error, invalid_point},
                 gen_influxdb_line:encode(#{measurement => 'cpu'})),
    ?assertEqual({error, invalid_type},
                 gen_influxdb_line:encode(#{measurement => 'cpu', fields => #{value => 1}, tags => #{host => 'serverA', region => 20}})),
    ok.

t_write(_) ->
    {ok, Pid} = gen_influxdb:start_link(),
    ok = gen_influxdb:write(Pid, [#{measurement => 'cpu',
                                    tags => #{host => 'serverA'},
                                    fields => #{value => 0.64}},
                                  #{measurement => 'temperature',
                                    tags => #{location => 'roomA'},
                                    fields => #{internal => 25, external => 37}}], [{set_timestamp, true}]).