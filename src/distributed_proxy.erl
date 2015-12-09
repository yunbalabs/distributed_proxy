%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 3:47 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy).
-author("zy").

%% API
-export([start/0, stop/0, join_cluster/1]).

start() ->
    application:ensure_started(?MODULE).

stop() ->
    application:stop(?MODULE).

join_cluster([Node]) ->
    join_cluster(Node);
join_cluster(Node) when is_list(Node)->
    join_cluster(list_to_atom(Node));
join_cluster(Node) ->
    case net_adm:ping(Node) of
        pong ->
            rpc:call(Node, distributed_proxy_ring_manager, add_node, [node()]);
        _ ->
            not_started
    end.