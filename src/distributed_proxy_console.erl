%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. 十二月 2015 10:35 AM
%%%-------------------------------------------------------------------
-module(distributed_proxy_console).
-author("zy").

%% API
-export([join_cluster/1, set_config/1, get_config/1, pause_replica/1,resume_replica/1]).

join_cluster([Node]) when is_list(Node) ->
    case distributed_proxy:join_cluster(list_to_atom(Node)) of
        ok ->
            io:format("Join ~p success~n",
                [Node]);
        Else ->
            io:format("Join ~p failed: ~p~n",
                [Node, Else])
    end,
    ok.

set_config([NameStr, IntegerStr]) ->
    Name = list_to_atom(NameStr),
    Value = list_to_integer(IntegerStr),
    lager:info("update config ~p=~p", [Name, Value]),
    ok = distributed_proxy_config:set(Name, Value),
    io:format("update ~p=~p success~n", [Name, Value]),
    ok.

get_config([NameStr]) ->
    Fun = list_to_atom(NameStr),
    io:format("~p~n", [distributed_proxy_config:Fun()]),
    ok.

pause_replica([IdxStr, GroupIndexStr]) ->
    Idx = list_to_integer(IdxStr),
    GroupIndex = list_to_integer(GroupIndexStr),
    ok = distributed_proxy_replica_manager:pause_replica(node(), {Idx, GroupIndex}),
    io:format("pause replica ~p_~p success~n", [Idx, GroupIndex]),
    ok.

resume_replica([IdxStr, GroupIndexStr]) ->
    Idx = list_to_integer(IdxStr),
    GroupIndex = list_to_integer(GroupIndexStr),
    ok = distributed_proxy_replica_manager:resume_replica(node(), {Idx, GroupIndex}),
    io:format("resume replica ~p_~p success~n", [Idx, GroupIndex]),
    ok.