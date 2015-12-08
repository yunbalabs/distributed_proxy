-module(distributed_proxy_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(CLI_MODULES, [distributed_proxy_cli]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    distributed_proxy_util:start_app_deps(distributed_proxy),

    F = fun() ->
        {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
        distributed_proxy_ring:get_all_nodes(MyRing)
        end,
    clique:register_node_finder(F),
    clique:register(?CLI_MODULES),

    distributed_proxy_sup:start_link().

stop(_State) ->
    ok.
