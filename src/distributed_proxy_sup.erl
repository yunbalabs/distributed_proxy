-module(distributed_proxy_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Timeout, Args), {I, {I, start_link, Args}, permanent, Timeout, Type, [I]}).
-define(CHILD(I, Type, Timeout), ?CHILD(I, Type, Timeout, [])).
-define(CHILD(I, Type), ?CHILD(I, Type, 5000)).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Children = lists:flatten([
        ?CHILD(distributed_proxy_status, worker),
        ?CHILD(distributed_proxy_replica_sup, supervisor, 305000),
        ?CHILD(distributed_proxy_ring_events, worker),
        ?CHILD(distributed_proxy_ring_manager, worker),
        ?CHILD(distributed_proxy_replica_proxy_sup, supervisor),
        ?CHILD(distributed_proxy_node_watcher, worker),
        ?CHILD(distributed_proxy_replica_manager, worker)
    ]),

    {ok, {{one_for_one, 10, 10}, Children}}.