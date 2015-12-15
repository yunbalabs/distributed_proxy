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
-export([
    slot_num/0, replica_size/0, check_replica_interval/0,
    replica_parallel_start_count/0, replica_module/0, warn_up_check_interval/0,
    warn_up_timeout/0,
    set/2, set/1, get/1]).

-define(DEFAULT_SLOT_NUM, 128).
-define(DEFAULT_REPLICA_SIZE, 2).
-define(DEFAULT_CHECK_REPLICA_INTERVAL, 10000).
-define(DEFAULT_REPLICA_PARALLEL_START_COUNT, 16).
-define(DEFAULT_REPLICA_MODULE, distributed_proxy_replica_example).
-define(DEFAULT_WARN_UP_CHECK_INTERVAL, 1000).   %% 1000 ms
-define(DEFAULT_WARN_UP_TIMEOUT, 600).           %% 10 min = 600 * 1000 ms

slot_num() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, slot_num, ?DEFAULT_SLOT_NUM).

replica_size() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, replica_size, ?DEFAULT_REPLICA_SIZE).

check_replica_interval() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, check_replica_interval, ?DEFAULT_CHECK_REPLICA_INTERVAL).

replica_parallel_start_count() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, replica_parallel_start_count, ?DEFAULT_REPLICA_PARALLEL_START_COUNT).

replica_module() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, replica_module, ?DEFAULT_REPLICA_MODULE).

warn_up_check_interval() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, warn_up_check_interval, ?DEFAULT_WARN_UP_CHECK_INTERVAL).

warn_up_timeout() ->
    {ok, App}  = application:get_application(?MODULE),
    application:get_env(App, warn_up_timeout, ?DEFAULT_WARN_UP_TIMEOUT).

set(check_replica_interval, Interval) when is_integer(Interval), Interval > 0 ->
    {ok, App}  = application:get_application(?MODULE),
    application:set_env(App, check_replica_interval, Interval);
set(warn_up_check_interval, Interval) when is_integer(Interval), Interval > 0 ->
    {ok, App}  = application:get_application(?MODULE),
    application:set_env(App, warn_up_check_interval, Interval),
    distributed_proxy_replica_sup:stop_all();
set(warn_up_timeout, Timeout) when is_integer(Timeout), Timeout > 0 ->
    {ok, App}  = application:get_application(?MODULE),
    application:set_env(App, warn_up_timeout, Timeout),
    distributed_proxy_replica_sup:stop_all().

set([NameStr, IntegerStr]) ->
    Name = list_to_atom(NameStr),
    Value = list_to_integer(IntegerStr),
    set(Name, Value).
get([NameStr]) ->
    Fun = list_to_atom(NameStr),
    io:format("~p~n", [?MODULE:Fun()]).