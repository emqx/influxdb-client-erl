-module(influxdb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [ t_encode_line
         , t_write
         , t_is_alive
         , t_check_auth
         ].

init_per_suite(Config) ->
    application:ensure_all_started(influxdb),
    Config.

end_per_suite(_Config) ->
    application:stop(influxdb).

init_per_testcase(t_write, _Config) ->
    {skip, {todo, refactor}};
init_per_testcase(_TestCase, Config) ->
    Config.

t_encode_line(_) ->
    ?assertEqual(<<"cpu value=0.64\n">>,
                 iolist_to_binary(influxdb_line:encode(#{measurement => 'cpu', fields => #{value => 0.64}}))),
    ?assertEqual(<<"cpu,host=serverA,region=us_west value=0.64\n">>,
                 iolist_to_binary(influxdb_line:encode(#{measurement => 'cpu', fields => #{value => 0.64}, tags => #{host => 'serverA', region => 'us_west'}}))),

    ?assertEqual(<<"cpu,host=serverA,region=us_west value=1i 100\n">>,
                 iolist_to_binary(influxdb_line:encode(#{measurement => 'cpu', fields => #{value => {int, 1}}, tags => #{host => 'serverA', region => 'us_west'}, timestamp => 100}))),

    ?assertEqual(<<"cpu,host=serverA,region=us_west value=1 100\n">>,
                 iolist_to_binary(influxdb_line:encode(#{measurement => 'cpu', fields => #{value => 1}, tags => #{host => 'serverA', region => 'us_west'}, timestamp => 100}))),


    %% special characters
    ?assertEqual(<<"cp\\ u v\\ a\\=l\\,ue=0.64\n">>,
                 iolist_to_binary(influxdb_line:encode(#{measurement => 'cp u', fields => #{"v a=l,ue" => 0.64}}))),

    ?assertException(error, {invalid_point, _}, influxdb_line:encode(#{measurement => 'cpu'})),
    ?assertException(error, {invalid_point, _}, influxdb_line:encode(#{measurement => 'cpu', field => #{value => 1}, tags => #{host => 'serverA', region => 20}})),

    ok.

t_write(_) ->
    t_write_(http, random, v1),
    t_write_(http, random, v2),
    t_write_(http, hash, v1),
    t_write_(http, hash, v2),
    t_write_(udp, random, v1),
    t_write_(udp, random, v2),
    t_write_(udp, hash, v1),
    t_write_(udp, hash, v2).

t_write_(WriteProtocol, PoolType, Version) ->
    Host = os:getenv("INFLUX_TCP_HOST", "influxdb_tcp"),
    Port = case WriteProtocol of
               http -> 8086;
               udp -> 8089
           end,
    Option = options(Host, Port, WriteProtocol, PoolType, Version),
    application:ensure_all_started(influxdb),
    {ok, Client} = influxdb:start_client(Option),
    timer:sleep(500),
    (WriteProtocol == udp) andalso (
        begin
            ?assertEqual(true, influxdb:is_alive(Client)),
            Points2 = [#{<<"fields">> => #{<<"temperature">> => 1},
                <<"measurement">> => <<"sample">>,
                <<"tags">> =>
                    #{<<"from">> => <<"mqttx_4b963a8e">>,<<"host">> => <<"serverA">>,
                <<"qos">> => 0,<<"region">> => <<"hangzhou">>},
                <<"timestamp">> => 1619775142098},
                #{<<"fields">> => #{<<"temperature">> => 2},
                <<"measurement">> => <<"sample">>,
                <<"tags">> =>
                    #{<<"from">> => <<"mqttx_4b963a8e">>,<<"host">> => <<"serverB">>,
                    <<"qos">> => 0,<<"region">> => <<"ningbo">>},
                <<"timestamp">> => 1619775142098}],
            case PoolType of
                hash ->
                    influxdb:write(Client, any_hash_key, Points2);
                _ ->
                    influxdb:write(Client, Points2)
            end
        end
    ),
    ok = influxdb:stop_client(Client).

t_is_alive(_) ->
    t_is_alive_(v1),
    t_is_alive_(v2).

t_is_alive_(Version) ->
    application:ensure_all_started(influxdb),
    Host = os:getenv("INFLUX_TCP_HOST", "influxdb_tcp"),
    Port0 = 8086,
    Option0 = options(Host, Port0, http, random, Version),
    {ok, Client0} = influxdb:start_client(Option0),
    timer:sleep(500),
    ?assertEqual(true, influxdb:is_alive(Client0)),
    ?assertEqual(true, influxdb:is_alive(Client0, true)),
    ok = influxdb:stop_client(Client0),

    Port1 = 27013, % dummy port, is_alive should return false
    Option1 = options(Host, Port1, http, random, Version),
    {ok, Client1} = influxdb:start_client(Option1),
    timer:sleep(500),
    ?assertEqual(false, influxdb:is_alive(Client1)),
    ?assertEqual({false, econnrefused}, influxdb:is_alive(Client1, true)),
    ok = influxdb:stop_client(Client1),

    {ok, Client2} = influxdb:start_client(Option0),
    % We remove the pool so that we will cause an exception
    BadClient = maps:remove(pool, Client2),
    timer:sleep(500),
    ?assertEqual(false, influxdb:is_alive(BadClient)),
    ?assertMatch({false, #{exception := error, reason := function_clause}},
                 influxdb:is_alive(BadClient, true)),
    ok = influxdb:stop_client(Client2).

t_check_auth(_) ->
    t_check_auth_(v1),
    t_check_auth_(v2).

t_check_auth_(Version) ->
    application:ensure_all_started(influxdb),
    Host = os:getenv("INFLUX_TCP_HOST", "influxdb_tcp"),
    Port = 8086,
    Option0 = options(Host, Port, http, random, Version),
    {ok, Client0} = influxdb:start_client(Option0),
    timer:sleep(500),
    ?assertEqual(ok, influxdb:check_auth(Client0)),
    ok = influxdb:stop_client(Client0),
    Option1 = options_wrong_credentials(Host, Port, http, random, Version),
    {ok, Client1} = influxdb:start_client(Option1),
    timer:sleep(500),
    ?assertEqual({error, not_authorized}, influxdb:check_auth(Client1)),
    ok = influxdb:stop_client(Client1).

options(Host, Port, WriteProtocol, PoolType, Version) when Version =:= v1 ->
    HttpsEnabled = false,
    UserName = <<"root">>,
    PassWord = <<"emqx@123">>,
    DataBase = <<"mqtt">>,
    Precision = <<"ns">>,
    Pool = <<"influxdb_test">>,
    PoolSize = 16,
    [ {host, Host}
    , {port, Port}
    , {protocol, WriteProtocol}
    , {https_enabled, HttpsEnabled}
    , {pool, Pool}
    , {pool_size, PoolSize}
    , {pool_type, PoolType}
    , {username, UserName}
    , {password, PassWord}
    , {database, DataBase}
    , {precision, Precision}
    , {version, Version}
    ];
options(Host, Port, WriteProtocol, PoolType, Version) when Version =:= v2 ->
    HttpsEnabled = false,
    Token = <<"abcdefg">>,
    Precision = <<"ns">>,
    Pool = <<"influxdb_test">>,
    PoolSize = 16,
    [ {host, Host}
    , {port, Port}
    , {protocol, WriteProtocol}
    , {https_enabled, HttpsEnabled}
    , {pool, Pool}
    , {pool_size, PoolSize}
    , {pool_type, PoolType}
    , {token, Token}
    , {precision, Precision}
    , {version, Version}
    ].

options_wrong_credentials(Host, Port, WriteProtocol, PoolType, Version) when Version =:= v1 ->
    Options = options(Host, Port, WriteProtocol, PoolType, Version),
    lists:keyreplace(password, 1, Options, {password, <<"wrong_password">>});
options_wrong_credentials(Host, Port, WriteProtocol, PoolType, Version) when Version =:= v2 ->
    Options = options(Host, Port, WriteProtocol, PoolType, Version),
    lists:keyreplace(token, 1, Options, {token, <<"wrong_token">>}).
