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
    replica_proxy_ping_interval/0, replica_proxy_check_interval/0, replica_proxy_overload_threshold/0,
    broadcast_interval/0,
    set/2, set/1, get/1]).

-define(DEFAULT_SLOT_NUM, 32).
-define(DEFAULT_REPLICA_SIZE, 2).
-define(DEFAULT_CHECK_REPLICA_INTERVAL, 10000).                         %% 10 s
-define(DEFAULT_REPLICA_PARALLEL_START_COUNT, 16).
-define(DEFAULT_REPLICA_MODULE, distributed_proxy_replica_example).
-define(DEFAULT_WARN_UP_CHECK_INTERVAL, 1000).                          %% 1000 ms
-define(DEFAULT_WARN_UP_TIMEOUT, 600).                                  %% 10 min = 600 * 1000 ms
-define(DEFAULT_REPLICA_PROXY_PING_INTERVAL, 2500).                     %% ping every 2500 times
-define(DEFAULT_REPLICA_PROXY_CHECK_INTERVAL, 5000).                    %% check more than 5000 times
-define(DEFAULT_REPLICA_PROXY_OVERLOAD_THRESHOLD, 10000).               %% overload more than 10000 times
-define(DEFAULT_BROADCAST_INTERVAL, 10000).                             %% 10 s

slot_num() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_SLOT_NUM;
        {ok, App} ->
            application:get_env(App, slot_num, ?DEFAULT_SLOT_NUM)
    end.

replica_size() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_REPLICA_SIZE;
        {ok, App} ->
            application:get_env(App, replica_size, ?DEFAULT_REPLICA_SIZE)
    end.

check_replica_interval() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_CHECK_REPLICA_INTERVAL;
        {ok, App} ->
            application:get_env(App, check_replica_interval, ?DEFAULT_CHECK_REPLICA_INTERVAL)
    end.

replica_parallel_start_count() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_REPLICA_PARALLEL_START_COUNT;
        {ok, App} ->
            application:get_env(App, replica_parallel_start_count, ?DEFAULT_REPLICA_PARALLEL_START_COUNT)
    end.

replica_module() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_REPLICA_MODULE;
        {ok, App} ->
            application:get_env(App, replica_module, ?DEFAULT_REPLICA_MODULE)
    end.

warn_up_check_interval() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_WARN_UP_CHECK_INTERVAL;
        {ok, App} ->
            application:get_env(App, warn_up_check_interval, ?DEFAULT_WARN_UP_CHECK_INTERVAL)
    end.

warn_up_timeout() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_WARN_UP_TIMEOUT;
        {ok, App} ->
            application:get_env(App, warn_up_timeout, ?DEFAULT_WARN_UP_TIMEOUT)
    end.

replica_proxy_ping_interval() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_REPLICA_PROXY_PING_INTERVAL;
        {ok, App} ->
            application:get_env(App, replica_proxy_ping_interval, ?DEFAULT_REPLICA_PROXY_PING_INTERVAL)
    end.

replica_proxy_check_interval() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_REPLICA_PROXY_CHECK_INTERVAL;
        {ok, App} ->
            application:get_env(App, replica_proxy_check_interval, ?DEFAULT_REPLICA_PROXY_CHECK_INTERVAL)
    end.

replica_proxy_overload_threshold() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_REPLICA_PROXY_OVERLOAD_THRESHOLD;
        {ok, App} ->
            application:get_env(App, replica_proxy_overload_threshold, ?DEFAULT_REPLICA_PROXY_OVERLOAD_THRESHOLD)
    end.

broadcast_interval() ->
    case application:get_application(?MODULE) of
        undefined ->
            ?DEFAULT_BROADCAST_INTERVAL;
        {ok, App} ->
            application:get_env(App, broadcast_interval, ?DEFAULT_BROADCAST_INTERVAL)
    end.

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
    distributed_proxy_replica_sup:stop_all();
set(replica_proxy_ping_interval, Interval) when is_integer(Interval), Interval > 0 ->
    {ok, App}  = application:get_application(?MODULE),
    application:set_env(App, replica_proxy_ping_interval, Interval),
    distributed_proxy_replica_proxy_sup:stop_all();
set(replica_proxy_check_interval, Interval) when is_integer(Interval), Interval > 0 ->
    {ok, App}  = application:get_application(?MODULE),
    application:set_env(App, replica_proxy_check_interval, Interval),
    distributed_proxy_replica_proxy_sup:stop_all();
set(replica_proxy_overload_threshold, Threshold) when is_integer(Threshold), Threshold > 0 ->
    {ok, App}  = application:get_application(?MODULE),
    application:set_env(App, replica_proxy_overload_threshold, Threshold),
    distributed_proxy_replica_proxy_sup:stop_all();
set(broadcast_interval, Interval) when is_integer(Interval), Interval > 0 ->
    {ok, App}  = application:get_application(?MODULE),
    application:set_env(App, broadcast_interval, Interval).

set([NameStr, IntegerStr]) ->
    Name = list_to_atom(NameStr),
    Value = list_to_integer(IntegerStr),
    lager:info("update config ~p=~p", [Name, Value]),
    set(Name, Value).

get([NameStr]) ->
    Fun = list_to_atom(NameStr),
    io:format("~p~n", [?MODULE:Fun()]).