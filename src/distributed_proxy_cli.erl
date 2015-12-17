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
    clique:register_usage(["riak-admin", "cluster", "status"], status_usage()),
    clique:register_usage(["riak-admin", "cluster", "replicas"], replicas_usage()),
    clique:register_usage(["riak-admin", "cluster", "map"], map_usage()),
    clique:register_usage(["riak-admin", "cluster", "replica"], replica_usage()).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
        [status_register(), replicas_register(), map_register(), replica_register()]).

cluster_usage() ->
    [
        "dp-admin cluster <sub-command>\n\n",
        "  Display cluster-related status and settings.\n\n",
        "  Sub-commands:\n",
        "    status           Display a summary of cluster status\n",
        "    replicas         Display replicas on a node\n",
        "    map              Display a map includes all replicas on the cluster\n",
        "    replica          Display a replica status on the specific node\n",
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

status(_CmdBase, [], []) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    AllNodes = distributed_proxy_ring:get_all_nodes(MyRing),

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
    [{node, Node}, {status, up}];
format_status(Node, false) ->
    [{node, Node}, {status, down}].

replicas_output(Node) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    Owners = distributed_proxy_ring:get_owners(MyRing),
    Rows = lists:map(
        fun ({Idx, GroupId}) ->
            Pos = distributed_proxy_ring:index2pos({Idx, GroupId}, MyRing),
            NodeGroup = distributed_proxy_ring:get_nodes(Pos, MyRing),
            IsMyReplica = lists:member(Node, NodeGroup),
            ReplicaPid =
                case distributed_proxy_util:safe_rpc(Node, distributed_proxy_replica_manager, get_replica_pid, [Idx], 30000) of
                    {badrpc, _} -> down;
                    {ok, Pid} ->
                        Pid;
                    not_found ->
                        down
                end,
            ProxyCounter =
                case IsMyReplica of
                    true ->
                        GroupIndex = distributed_proxy_util:index_of(Node, NodeGroup),
                        Proxy = distributed_proxy_util:replica_proxy_reg_name(list_to_binary(lists:flatten(io_lib:format("~w_~w", [Idx, GroupIndex])))),
                        case catch sys:get_status({Proxy, Node}, 30000) of
                            {status, _, _, [_, _, _, _, {state, _, _, _, _, Counter, _, _, _, _}]} ->
                                Counter;
                            Error ->
                                lager:error("get proxy status failed ~p", [Error]),
                                0
                        end;
                    false ->
                        0
                end,

            [{replica, Idx}, {pid, ReplicaPid}, {proxy_counter, ProxyCounter}]
        end, Owners),

    T0 = clique_status:text(io_lib:format("Replicas on ~p:", [Node])),
    Table = clique_status:table(Rows),
    [T0, Table].

replica_output(Node, Replica) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    Owners = distributed_proxy_ring:get_owners(MyRing),
    case lists:keyfind(Replica, 1, Owners) of
        {Replica, GroupId} ->
            Pos = distributed_proxy_ring:index2pos({Replica, GroupId}, MyRing),
            NodeGroup = distributed_proxy_ring:get_nodes(Pos, MyRing),
            IsMyReplica = lists:member(Node, NodeGroup),

            {Status, Pid} =
                case distributed_proxy_util:safe_rpc(Node, distributed_proxy_replica_manager, get_replica_pid, [Replica], 30000) of
                    {badrpc, _} -> {down, down};
                    {ok, P} ->
                        case catch sys:get_status(P, 30000) of
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
                        {down, down}
                end,

            Rows = [[{own, IsMyReplica}, {status, Status}, {pid, Pid}]],

            Table = clique_status:table(Rows),

            T0 = clique_status:text(io_lib:format("Replica ~p on ~p:", [Replica, Node])),
            [T0, Table];
        false ->
            [clique_status:alert([clique_status:text("Cannot find the replica.")])]
    end.