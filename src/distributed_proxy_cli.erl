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
-export([register_cli/0]).

register_cli() ->
    register_all_usage(),
    register_all_commands().

register_all_usage() ->
    clique:register_usage(["dp-admin", "cluster"], cluster_usage()).

register_all_commands() ->
    lists:foreach(fun(Args) -> apply(clique, register_command, Args) end,
        [status_register()]).

cluster_usage() ->
    [
        "dp-admin cluster <sub-command>\n\n",
        "  Display cluster-related status and settings.\n\n",
        "  Sub-commands:\n",
        "    status           Display a summary of cluster status\n",
        "    partitions       Display partitions on a node\n",
        "  Use --help after a sub-command for more details.\n"
    ].

status_register() ->
    [["dp-admin", "cluster", "status"],      % Cmd
        [],                                  % KeySpecs
        [],                                  % FlagSpecs
        fun status/3].                       % Implementation callback.

status(_CmdBase, [], []) ->
    T2 = clique_status:text("test"),
    [T2].