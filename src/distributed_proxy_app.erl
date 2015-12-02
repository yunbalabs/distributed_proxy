-module(distributed_proxy_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    distributed_proxy_util:start_app_deps(distributed_proxy),
    distributed_proxy_sup:start_link().

stop(_State) ->
    ok.
