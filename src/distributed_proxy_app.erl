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

    case distributed_proxy_sup:start_link() of
        {ok, SupPid} ->
            {ok, Ring} = distributed_proxy_ring_manager:get_ring(),
            distributed_proxy_ring_events:ring_sync_update(Ring),
            {ok, SupPid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
