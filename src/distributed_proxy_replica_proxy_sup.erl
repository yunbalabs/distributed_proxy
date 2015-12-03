%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 2:49 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_replica_proxy_sup).
-author("zy").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init([]) ->
    Indices = get_indexes(),
    Proxies = [proxy_ref(Index) || Index <- Indices],
    {ok, {{one_for_one, 5, 10}, Proxies}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
proxy_ref({IndexName, Idx, GroupIndex}) ->
    {IndexName, {distributed_proxy_replica_proxy, start_link, [IndexName, Idx, GroupIndex]},
        permanent, 5000, worker, [distributed_proxy_replica_proxy]}.

get_indexes() ->
    {ok, Ring} = distributed_proxy_ring_manager:get_ring(),
    AllOwners = distributed_proxy_ring:get_owners(Ring),
    ReplicaSize = distributed_proxy_config:replica_size(),
    ReplicaSeq = lists:seq(1, ReplicaSize),
    lists:flatten([
        [{list_to_binary(lists:flatten(io_lib:format("~w_~w", [Idx, GroupIndex]))), Idx, GroupIndex} || GroupIndex <- ReplicaSeq]
        || {Idx, _} <- AllOwners
    ]).