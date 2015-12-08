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
    clique:register_usage(["riak-admin", "cluster", "replicas"], replicas_usage()).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
        [status_register(), replicas_register()]).

cluster_usage() ->
    [
        "dp-admin cluster <sub-command>\n\n",
        "  Display cluster-related status and settings.\n\n",
        "  Sub-commands:\n",
        "    status           Display a summary of cluster status\n",
        "    replicas         Display replicas on a node\n",
        "  Use --help after a sub-command for more details.\n"
    ].

status_usage() ->
    ["dp-admin cluster status\n\n",
        "  Display a summary of cluster status information.\n"].

replicas_usage() ->
    ["dp-admin cluster replicas [--node node]\n\n",
        "  Display the replicas on a node. Defaults to local node.\n"].

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

format_status(Node, true) ->
    [{node, Node}, {status, up}];
format_status(Node, false) ->
    [{node, Node}, {status, down}].

replicas_output(Node) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    Owners = distributed_proxy_ring:get_owners(MyRing),
    Rows = lists:map(
        fun ({Idx, GroupId}) ->
            NodeGroup = distributed_proxy_ring:get_nodes(GroupId, MyRing),
            IsMyReplica = lists:member(Node, NodeGroup),
            ReplicaState =
                case distributed_proxy_util:safe_rpc(Node, distributed_proxy_replica_manager, get_replica_pid, [Idx], 30000) of
                    {badrpc, _} -> down;
                    {ok, Pid} ->
                        case catch sys:get_status(Pid, 30000) of
                            {status, _, _, [_, _, _, _, [_, {data, State} | _]]} ->
                                case lists:keyfind("StateName", 1, State) of
                                    {"StateName", CurrentState} ->
                                        CurrentState;
                                    false ->
                                        down
                                end;
                            _Error ->
                                down
                        end;
                    not_found ->
                        down
                end,

            [{replica, Idx}, {own, IsMyReplica}, {status, ReplicaState}]
        end, Owners),

    T0 = clique_status:text(io_lib:format("Replicas on ~p:", [Node])),
    Table = clique_status:table(Rows),
    [T0, Table].