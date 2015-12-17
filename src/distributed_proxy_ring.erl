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
-export([
    create/0, create/1,
    add_node/2,
    get_chashbin/1, index2pos/2, get_nodes/2, get_all_nodes/1, get_owners/1, get_changes/2,
    complete_change/3
]).

-record(state, {
    raw_ring,
    node_group,
    free_node,
    slot_num,
    replica_size,
    chashbin,
    node_map,
    next,
    node_map_dict
}).

create() ->
    create(node()).

create(NodeName) ->
    SlotNum = distributed_proxy_config:slot_num(),
    ReplicaSize = distributed_proxy_config:replica_size(),
    Ring = ring:create(SlotNum, 0),
    NodeGroup = [{0, lists:duplicate(ReplicaSize, NodeName)}],
    NodeMap = [{Pos, lists:duplicate(ReplicaSize, NodeName)} || Pos <- [0 | lists:seq(1, SlotNum - 1)]],
    FreeNode = case ReplicaSize of
                   1 ->
                       [];
                   _ ->
                       [NodeName]
               end,
    #state{
        raw_ring = Ring,
        node_group = NodeGroup, free_node = FreeNode,
        node_map = NodeMap, next = [], node_map_dict = dict:from_list(NodeMap),
        slot_num = SlotNum, replica_size = ReplicaSize,
        chashbin = ring:to_bin(Ring)
    }.

add_node(_NodeName, State = #state{next = Next}) when length(Next) > 0 ->
    {still_reconciling, State};
add_node(NodeName, State = #state{replica_size = 1}) ->
    NewState = reconcile(State#state{free_node = [NodeName]}),
    {ok, NewState};
add_node(NodeName, State = #state{free_node = FreeNode, replica_size = ReplicaSize}) ->
    NewState =
        case length(FreeNode) of
            ReplicaSize ->
                State2 = reconcile(State),
                add_node(NodeName, State2);
            CurrentSize when ReplicaSize - CurrentSize =:= 1 ->
                reconcile(State#state{free_node = FreeNode ++ [NodeName]});
            CurrentSize when CurrentSize < ReplicaSize ->
                State#state{free_node = FreeNode ++ [NodeName]}
        end,
    {ok, NewState}.

get_chashbin(#state{chashbin = CHBin}) ->
    CHBin.

index2pos({Idx, GroupId}, #state{raw_ring = Ring}) ->
    distributed_proxy_util:index_of({Idx, GroupId}, ring:owners(Ring)) - 1.

get_nodes(Pos, #state{node_map_dict = NodeMap}) ->
    case dict:find(Pos, NodeMap) of
        {ok, Nodes} ->
            Nodes;
        error ->
            []
    end.

get_all_nodes(#state{node_group = NodeGroup}) ->
    lists:usort(lists:foldl(
        fun({_GroupId, Nodes}, Acc) ->
            lists:merge([Nodes, Acc])
        end,
        [], NodeGroup)).

get_owners(#state{raw_ring = Ring}) ->
    ring:owners(Ring).

get_changes(Node, #state{node_group = NodeGroup, next = Next}) ->
    lists:filtermap(
        fun({Idx, _OldGroupId, NewGroupId}) ->
            {NewGroupId, Nodes} = lists:keyfind(NewGroupId, 1, NodeGroup),
            case distributed_proxy_util:index_of(Node, Nodes) of
                not_found ->
                    false;
                GroupIndex ->
                    {true, {Idx, GroupIndex}}
            end
        end, Next).

complete_change(Node, {Idx, GroupIndex}, State = #state{raw_ring = Ring, node_group = NodeGroup, next = Next, node_map = NodeMap}) ->
    GroupId = ring:idx2node(Ring, Idx),
    Pos = distributed_proxy_util:index_of({Idx, GroupId}, ring:owners(Ring)) - 1,
    {Pos, OldNodes} = lists:keyfind(Pos, 1, NodeMap),
    NewNodes = distributed_proxy_util:setnth(GroupIndex, OldNodes, Node),
    NewNodeMap = lists:keyreplace(Pos, 1, NodeMap, {Pos, NewNodes}),
    {GroupId, WantedNodes} = lists:keyfind(GroupId, 1, NodeGroup),
    NewNext =
        case NewNodes =:= WantedNodes of
            true ->
                lists:keydelete(Idx, 1, Next);
            false ->
                Next
        end,
    State#state{next = NewNext, node_map = NewNodeMap, node_map_dict = dict:from_list(NewNodeMap)}.

%% reconcile the ring
reconcile(State = #state{
    node_group = [{0, [Node0, Node0 | _]}], free_node = FreeNode, replica_size = ReplicaSize,
    slot_num = SlotNum
}) when length(FreeNode) =:= ReplicaSize ->
    NodeMap = [{Pos, FreeNode} || Pos <- [0 | lists:seq(1, SlotNum - 1)]],
    State#state{
        node_group = [{0, FreeNode}],       %% for the first reconcile, just overwrite the node_group
        node_map = NodeMap,
        node_map_dict = dict:from_list(NodeMap),
        free_node = []
    };
reconcile(State = #state{
    raw_ring = Ring, node_group = NodeGroup, free_node = FreeNode, replica_size = ReplicaSize
}) when length(FreeNode) =:= ReplicaSize ->
    GroupId = length(NodeGroup),
    Ring2 = ring:add_node(Ring, GroupId),
    NodeGroup2 = [{GroupId, FreeNode} | NodeGroup],
    Next = ring:diff(Ring, Ring2),
    State#state{
        raw_ring = Ring2,
        node_group = NodeGroup2,
        free_node = [],
        next = Next,
        chashbin = ring:to_bin(Ring2)
    };
reconcile(State) ->
    State.