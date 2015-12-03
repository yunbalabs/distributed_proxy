%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 4:41 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_ring).
-author("zy").

%% API
-export([create/0, create/1, add_node/2, get_chashbin/1, get_nodes/2, get_all_nodes/1, get_owners/1]).

-record(state, {
    raw_ring,
    node_group,
    free_node,
    slot_num,
    replica_size,
    chashbin,
    node_group_dict
}).

create() ->
    create(node()).

create(NodeName) ->
    SlotNum = distributed_proxy_config:slot_num(),
    ReplicaSize = distributed_proxy_config:replica_size(),
    Ring = ring:create(SlotNum, 0),
    NodeGroup = [{0, lists:duplicate(ReplicaSize, NodeName)}],
    FreeNode = case ReplicaSize of
                   1 ->
                       [];
                   _ ->
                       [NodeName]
               end,
    #state{
        raw_ring = Ring,
        node_group = NodeGroup, free_node = FreeNode,
        slot_num = SlotNum, replica_size = ReplicaSize,
        chashbin = ring:to_bin(Ring), node_group_dict = dict:from_list(NodeGroup)
    }.

add_node(NodeName, State = #state{replica_size = 1}) ->
    reconcile(State#state{free_node = [NodeName]});
add_node(NodeName, State = #state{free_node = FreeNode, replica_size = ReplicaSize}) ->
    case length(FreeNode) of
        ReplicaSize ->
            State2 = reconcile(State),
            add_node(NodeName, State2);
        CurrentSize when ReplicaSize - CurrentSize =:= 1 ->
            reconcile(State#state{free_node = [NodeName | FreeNode]});
        CurrentSize when CurrentSize < ReplicaSize ->
            State#state{free_node = [NodeName | FreeNode]}
    end.

get_chashbin(#state{chashbin = CHBin}) ->
    CHBin.

get_nodes(GroupId, #state{node_group_dict = NodeGroup}) ->
    case dict:find(GroupId, NodeGroup) of
        {ok, Nodes} ->
            Nodes;
        error ->
            []
    end.

get_all_nodes(#state{node_group = NodeGroup}) ->
    lists:foldl(
        fun({_GroupId, Nodes}, Acc) ->
            lists:merge([Nodes, Acc])
        end,
        [], NodeGroup).

get_owners(#state{raw_ring = Ring}) ->
    ring:owners(Ring).

%% reconcile the ring
reconcile(State = #state{
    node_group = [{0, [Node0, Node0 | _]}], free_node = FreeNode, replica_size = ReplicaSize
}) when length(FreeNode) =:= ReplicaSize ->
    State#state{
        node_group = [{0, FreeNode}],       %% for the first reconcile, just overwrite the node_group
        free_node = [],
        node_group_dict = dict:from_list([{0, FreeNode}])
    };
reconcile(State = #state{
    raw_ring = Ring, node_group = NodeGroup, free_node = FreeNode, replica_size = ReplicaSize
}) when length(FreeNode) =:= ReplicaSize ->
    GroupId = length(NodeGroup),
    %% TODO: change the ring after these replica active
    Ring2 = ring:add_node(Ring, GroupId),
    NodeGroup2 = [{GroupId, FreeNode} | NodeGroup],
    State#state{
        raw_ring = Ring2,
        node_group = NodeGroup2,
        free_node = [],
        chashbin = ring:to_bin(Ring2),
        node_group_dict = dict:from_list(NodeGroup2)
    };
reconcile(State) ->
    State.