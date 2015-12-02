%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 7:22 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_config).
-author("zy").

%% API
-export([slot_num/0, replica_size/0, check_replica_interval/0, replica_parallel_start_count/0]).

-define(DEFAULT_SLOT_NUM, 128).
-define(DEFAULT_REPLICA_SIZE, 2).
-define(DEFAULT_REPLICA_INTERVAL, 10000).
-define(DEFAULT_REPLICA_PARALLEL_START_COUNT, 16).

slot_num() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, slot_num, ?DEFAULT_SLOT_NUM).

replica_size() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, replica_size, ?DEFAULT_REPLICA_SIZE).

check_replica_interval() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, check_replica_interval, ?DEFAULT_REPLICA_INTERVAL).

replica_parallel_start_count() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, replica_parallel_start_count, ?DEFAULT_REPLICA_PARALLEL_START_COUNT).