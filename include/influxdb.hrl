%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% defaults
-define(DEFAULT_WRITE_PROTOCOL, udp).
-define(DEFAULT_BATCH_SIZE, 32).
-define(DEFAULT_UDP_OPTS, [{host, "127.0.0,1"},
                           {port, 8089}]).
-define(DEFAULT_HTTP_OPTS, [{host, "127.0.0.1"},
                            {port, 8086},
                            {database, "mydb"},
                            {precision, ms},
                            {https_enabled, false}]).

%% types
-type precision() :: ns | us | ms | s | m | h.
-type udp_opts() :: [udp_opt()].
-type udp_opt() :: {host, inet:ip_address() | inet:hostname()}
                 | {port, inet:port_number()}.
-type http_opts() :: [http_opt()].
-type http_opt() :: {host, inet:hostname()}
                  | {port, inet:port_number()}
                  | {database, string()}
                  | {username, string()}
                  | {password, string()}
                  | {precision, precision()}
                  | {https_enabled, boolean()}
                  | {ssl, ssloptions()}.
-type ssloptions() :: [ssloption()].
-type ssloption() :: {versions, [ssl:protocol_version()]}
                   | {keyfile, file:filename()}
                   | {certfile, file:filename()}
                   | {cacertfile, file:filename()}
                   | {ciphers, string()}.