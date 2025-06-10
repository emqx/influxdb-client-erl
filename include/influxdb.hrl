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

-type ssloptions() :: [ssl:tls_client_option()].

-type(kv() :: {Key :: atom() | string() | binary(), Value :: atom() | string() | integer() | binary()}).

-type(tag()     :: kv()).
-type(field()   :: kv()).

-type(tags()    :: [tag()]).
-type(fields()  :: [field()]).

-define(VALUES_BOOLEAN, 
    #{
        't' => <<"t">>,
        'T' => <<"T">>,
        'true' => <<"true">>,
        'TRUE' => <<"TRUE">>,
        'True' => <<"True">>,
        'f' => <<"f">>,
        'F' => <<"F">>,
        'false' => <<"false">>,
        'FALSE' => <<"FALSE">>,
        'False' => <<"False">>
    }
).
