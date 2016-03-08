%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. 十二月 2015 7:32 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_cli).
-author("zy").

-behaviour(clique_handler).

%% API
-export([register_cli/0, command/1]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

command(Cmd) ->
    clique:run(Cmd).

register_all_usage() ->
    clique:register_usage(["dp-admin", "cluster"], cluster_usage()),
    clique:register_usage(["dp-admin", "cluster", "status"], status_usage()),
    clique:register_usage(["dp-admin", "cluster", "replicas"], replicas_usage()),
    clique:register_usage(["dp-admin", "cluster", "map"], map_usage()),
    clique:register_usage(["dp-admin", "cluster", "replica"], replica_usage()),
    clique:register_usage(["dp-admin", "cluster", "locate"], locate_usage()).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
        [status_register(), replicas_register(), map_register(), replica_register(), locate_register()]).

cluster_usage() ->
    [
        "dp-admin cluster <sub-command>\n\n",
        "  Display cluster-related status and settings.\n\n",
        "  Sub-commands:\n",
        "    status           Display a summary of cluster status\n",
        "    replicas         Display replicas on a node\n",
        "    map              Display a map includes all replicas on the cluster\n",
        "    replica          Display a replica status on the specific node\n",
        "    locate           Display replicas status about the specific key\n",
        "  Use --help after a sub-command for more details.\n"
    ].

status_usage() ->
    ["dp-admin cluster status\n\n",
        "  Display a summary of cluster status information.\n"].

replicas_usage() ->
    ["dp-admin cluster replicas [--node node]\n\n",
        "  Display the replicas on a node. Defaults to local node.\n"].

map_usage() ->
    ["dp-admin cluster map\n\n",
        "  Display a map includes all replicas on the cluster.\n"].

replica_usage() ->
    ["dp-admin cluster replica --replica replica_id --node node\n\n",
        "  Display a replica status on the specific node.\n"].

locate_usage() ->
    ["dp-admin cluster locate --key key\n\n",
        "  Display replicas status about the specific key.\n"].

status_register() ->
    [["dp-admin", "cluster", "status"],      % Cmd
        [],                                  % KeySpecs
        [],                                  % FlagSpecs
        fun status/3].                       % Implementation callback.

replicas_register() ->
    [["dp-admin", "cluster", "replicas"],             % Cmd
        [],                                           % KeySpecs
        [{node, [{shortname, "n"}, {longname, "node"},
            {typecast,
                fun clique_typecast:to_node/1}]}],    % FlagSpecs
        fun replicas/3].                              % Implementation callback

map_register() ->
    [["dp-admin", "cluster", "map"],         % Cmd
        [],                                  % KeySpecs
        [],                                  % FlagSpecs
        fun map/3].                          % Implementation callback.

replica_register() ->
    [["dp-admin", "cluster", "replica"],              % Cmd
        [],                                           % KeySpecs
        [
            {replica, [{shortname, "r"}, {longname, "replica"},
                {typecast,
                    fun list_to_integer/1}]},
            {node, [{shortname, "n"}, {longname, "node"},
                {typecast,
                    fun clique_typecast:to_node/1}]}
        ],    % FlagSpecs
        fun replica/3].                              % Implementation callback

locate_register() ->
    [["dp-admin", "cluster", "locate"],               % Cmd
        [],                                           % KeySpecs
        [
            {key, [{shortname, "k"}, {longname, "key"},
                {typecast,
                    fun list_to_binary/1}]}
        ],    % FlagSpecs
        fun locate/3].                               % Implementation callback

status(_CmdBase, [], []) ->
    {ok, Peers} = distributed_proxy_node_watcher:peers(),
    AllNodes = lists:usort([node() | Peers]),

    Rows =[format_status(Node, distributed_proxy_node_watcher:is_up(Node)) || Node <- AllNodes ],

    Table = clique_status:table(Rows),

    T0 = clique_status:text("---- Cluster Status ----"),
    [T0, Table].

replicas(_CmdBase, [], [{node, Node}]) ->
    replicas_output(Node);
replicas(_CmdBase, [], []) ->
    replicas_output(node()).

replica(_CmdBase, [], [{replica, Replica}, {node, Node}]) ->
    replica_output(Node, Replica);
replica(_CmdBase, [], [{replica, Replica}]) ->
    replica_output(node(), Replica).

locate(_CmdBase, [], [{key, KeyBin}]) ->
    locate_output(KeyBin).

map(_CmdBase, [], []) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    Owners = distributed_proxy_ring:get_owners(MyRing),

    Rows = lists:map(
        fun ({Idx, GroupId}) ->
            Pos = distributed_proxy_ring:index2pos({Idx, GroupId}, MyRing),
            NodeGroup = distributed_proxy_ring:get_nodes(Pos, MyRing),
            ReplicaMap = [{distributed_proxy_util:index_of(Node, NodeGroup), Node} || Node <- NodeGroup],
            [{replica, Idx} | ReplicaMap]
        end, Owners),

    Table = clique_status:table(Rows),

    T0 = clique_status:text("---- Cluster Map ----"),
    [T0, Table].

format_status(Node, true) ->
    case distributed_proxy_util:safe_rpc(Node, distributed_proxy_ring_manager, get_ring, [], 3000) of
        {badrpc, _} ->
            [{node, Node}, {status, unknown}];
        {error, _Error} ->
            [{node, Node}, {status, unknown}];
        {ok, Ring} ->
            case distributed_proxy_ring:get_all_changes(Ring) of
                [] ->
                    [{node, Node}, {status, up}];
                _ ->
                    [{node, Node}, {status, reconciling}]
            end
    end;
format_status(Node, false) ->
    [{node, Node}, {status, down}].

replicas_output(Node) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    Owners = distributed_proxy_ring:get_owners(MyRing),

    FetchFun =
        fun (Idx, GroupIndex) ->
            ReplicaPid =
                case distributed_proxy_util:safe_rpc(Node, distributed_proxy_replica_manager, get_replica_pid, [{Idx,GroupIndex}], 10000) of
                    {badrpc, _} -> not_found;
                    {ok, Pid} ->
                        Pid;
                    not_found ->
                        not_found
                end,

            ReplicaStatus =
                case distributed_proxy_util:safe_rpc(Node, distributed_proxy_status, replica_state, [{Idx, GroupIndex}], 10000) of
                    {badrpc, _} -> down;
                    Status ->
                        Status
                end,

            Proxy = distributed_proxy_util:replica_proxy_reg_name(list_to_binary(lists:flatten(io_lib:format("~w_~w", [Idx, GroupIndex])))),
            case catch sys:get_status({Proxy, Node}, 10000) of
                {status, _, _, [_, _, _, _, {state, _, _, _, _, Counter, _, _, _, _}]} ->
                    {ReplicaStatus, ReplicaPid, Counter};
                Error ->
                    lager:error("get proxy status failed ~p", [Error]),
                    {ReplicaStatus, ReplicaPid, 0}
            end
        end,

    Rows = lists:map(
        fun ({Idx, GroupId}) ->
            Pos = distributed_proxy_ring:index2pos({Idx, GroupId}, MyRing),
            NodeGroup = distributed_proxy_ring:get_nodes(Pos, MyRing),
            {IsMyReplica, ReplicaStatus, ReplicaPid, ProxyCounter} =
                case distributed_proxy_util:index_of(Node, NodeGroup) of
                    not_found ->
                        IdealNodes = distributed_proxy_ring:get_ideal_nodes(GroupId, MyRing),
                        case distributed_proxy_util:index_of(Node, IdealNodes) of
                            not_found ->
                                {no, down, not_found, 0};
                            IdealGroupIndex ->
                                {RS, RP, C} = FetchFun(Idx, IdealGroupIndex),
                                {future, RS, RP, C}
                        end;
                    GroupIndex ->
                        {RS, RP, C} = FetchFun(Idx, GroupIndex),
                        {yes, RS, RP, C}
                end,

            [{replica, Idx}, {own, IsMyReplica}, {status, ReplicaStatus}, {pid, ReplicaPid}, {proxy_counter, ProxyCounter}]
        end, Owners),

    T0 = clique_status:text(io_lib:format("Replicas on ~p:", [Node])),
    Table = clique_status:table(Rows),
    [T0, Table].

replica_output(Node, Replica) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    Owners = distributed_proxy_ring:get_owners(MyRing),

    FetchFun =
        fun (Idx, GroupIndex) ->
            case distributed_proxy_util:safe_rpc(Node, distributed_proxy_replica_manager, get_replica_pid, [{Idx, GroupIndex}], 10000) of
                {badrpc, _} -> {down, not_found};
                {ok, P} ->
                    case catch sys:get_status(P, 10000) of
                        {status, _, _, [_, _, _, _, [_, {data, State} | _]]} ->
                            case lists:keyfind("StateName", 1, State) of
                                {"StateName", CurrentState} ->
                                    {CurrentState, P};
                                false ->
                                    {unknown, P}
                            end;
                        _Error ->
                            {unknown, P}
                    end;
                not_found ->
                    {down, not_found}
            end
        end,

    case lists:keyfind(Replica, 1, Owners) of
        {Replica, GroupId} ->
            Pos = distributed_proxy_ring:index2pos({Replica, GroupId}, MyRing),
            NodeGroup = distributed_proxy_ring:get_nodes(Pos, MyRing),

            {IsMyReplica, Status, Pid} =
                case distributed_proxy_util:index_of(Node, NodeGroup) of
                    not_found ->
                        IdealNodes = distributed_proxy_ring:get_ideal_nodes(GroupId, MyRing),
                        case distributed_proxy_util:index_of(Node, IdealNodes) of
                            not_found ->
                                {no, down, not_found};
                            IdealGroupIndex ->
                                {S, P} = FetchFun(Replica, IdealGroupIndex),
                                {future, S, P}
                        end;
                    GroupIndex ->
                        {S, P} = FetchFun(Replica, GroupIndex),
                        {yes, S, P}
                end,

            Rows = [[{own, IsMyReplica}, {status, Status}, {pid, Pid}]],

            Table = clique_status:table(Rows),

            T0 = clique_status:text(io_lib:format("Replica ~p on ~p:", [Replica, Node])),
            [T0, Table];
        false ->
            [clique_status:alert([clique_status:text("Cannot find the replica.")])]
    end.

locate_output(KeyBin) ->
    {ok, Ring} = distributed_proxy_ring_manager:get_ring(),
    {Idx, Pos, _GroupId} = ring:locate_key(distributed_proxy_ring:get_chashbin(Ring), KeyBin),
    Nodes = distributed_proxy_ring:get_nodes(Pos, Ring),
    Rows = lists:map(
        fun (Node) ->
            GroupIndex = distributed_proxy_util:index_of(Node, Nodes),
            Proxy = distributed_proxy_util:replica_proxy_reg_name(list_to_binary(lists:flatten(io_lib:format("~w_~w", [Idx, GroupIndex])))),
            ProxyCounter =
                case catch sys:get_status({Proxy, Node}, 10000) of
                    {status, _, _, [_, _, _, _, {state, _, _, _, _, Counter, _, _, _, _}]} ->
                        Counter;
                    Error ->
                        lager:error("get proxy status failed ~p", [Error]),
                        0
                end,
            {ReplicaState, ReplicaPid} =
                case distributed_proxy_util:safe_rpc(Node, distributed_proxy_replica_manager, get_replica_pid, [{Idx, GroupIndex}], 10000) of
                    {badrpc, _} -> {down, not_found};
                    {ok, P} ->
                        case catch sys:get_status(P, 10000) of
                            {status, _, _, [_, _, _, _, [_, {data, State} | _]]} ->
                                case lists:keyfind("StateName", 1, State) of
                                    {"StateName", CurrentState} ->
                                        {CurrentState, P};
                                    false ->
                                        {unknown, P}
                                end;
                            _Error ->
                                {unknown, P}
                        end;
                    not_found ->
                        {down, not_found}
                end,
            [{node, Node}, {pid, ReplicaPid}, {status, ReplicaState}, {proxy_counter, ProxyCounter}]
        end, Nodes),

    T0 = clique_status:text(io_lib:format("The key ~p is on replica ~p:", [KeyBin, Idx])),
    Table = clique_status:table(Rows),
    [T0, Table].