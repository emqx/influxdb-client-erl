# Influxdb client erl

Client for influxdb

## Exampler Code

Start application

``` erlang
application:ensure_all_started(influxdb).
```

Client Options: Write protocol `http` or `udp`

``` erlang
%% http
Option = [ {host, "127.0.0.1"}
         , {port, 8086}
         , {protocol, http}
         , {https_enabled, false}
         , {pool, influxdb_client_pool}
         , {pool_size, 8}
         , {pool_type, random}
         , {username, <<"uname">>}
         , {password, <<"'pwd'">>}
         , {database, <<"mydb">>}
         , {precision, <<"ms">>}].
```

``` erlang
%% udp
Option = [ {host, "127.0.0.1"}
         , {port, 8089}
         , {protocol, udp}
         , {pool, influxdb_client_pool}
         , {pool_size, 8}
         , {pool_type, random}].
```

Start client

``` erlang
{ok, Client} = influxdb:start_client(Option).
```

Make sure client alive

``` erlang
true = influxdb:is_alive(Client).
```

Test data

``` erlang
Points = [#{<<"fields">> => #{<<"temperature">> => 1},
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
            <<"timestamp">> => 1619775142098}].
```

Write to influxdb

``` erlang
influxdb:write(Client, Points).
```

If your `pool_type` is `hash`

``` erlang
Key = any_key_you_need.
influxdb:write(Client, Key, Points).
```

Stop Client

``` erlang
influxdb:stop_client(Client).
```
