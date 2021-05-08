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
-module(influxdb_udp).

-export([ write/2
        , write/3]).

write(#{pool := Pool}, Data) ->
    Fun = fun(Worker) ->
              influxdb_worker_udp:write(Worker, Data)
          end,
    try ecpool:with_client(Pool, Fun)
    catch E:R:S ->
        logger:error("[InfluxDB] http write fail: ~0p ~0p ~0p", [E, R, S]),
        {error, {E, R}}
    end.

write(#{pool := Pool}, Key, Data) ->
    Fun = fun(Worker) ->
              influxdb_worker_udp:write(Worker, Data)
          end,
    try ecpool:with_client(Pool, Key, Fun)
    catch E:R:S ->
        logger:error("[InfluxDB] http write fail: ~0p ~0p ~0p", [E, R, S]),
        {error, {E, R}}
    end.
