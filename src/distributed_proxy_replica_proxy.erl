%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 3:02 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_replica_proxy).
-author("zy").

%% API
-export([start_link/3, get_my_replica_pid/1, call/2, call/3]).

%% callbacks
-export([init/1]).

-record(state, {
    index, group_index,
    replica_pid = undefined,
    replica_monitor
}).

-include("distributed_proxy_replica.hrl").

start_link(IndexName, Idx, GroupIndex) ->
    proc_lib:start_link(?MODULE, init, [[self(), IndexName, Idx, GroupIndex]]).

get_my_replica_pid(ProxyName) ->
    call(ProxyName, get_replica_pid).

call(Name, Msg) ->
    call_reply(catch gen:call(Name, '$replica_proxy_call', Msg)).

call(Name, Msg, Timeout) ->
    call_reply(catch gen:call(Name, '$replica_proxy_call', Msg, Timeout)).

-spec call_reply({atom(), term()}) -> term().
call_reply({ok, Res}) ->
    Res;
call_reply({'EXIT', Reason}) ->
    {error, Reason}.

init([Parent, IndexName, Idx, GroupIndex]) ->
    RegName = distributed_proxy_util:replica_proxy_reg_name(IndexName),
    erlang:register(RegName, self()),
    proc_lib:init_ack(Parent, {ok, self()}),

    loop(Parent, #state{index = Idx, group_index = GroupIndex}).

%%%===================================================================
%%% Internal functions
%%%===================================================================
loop(Parent, State) ->
    receive
        {'$replica_proxy_call', From, Msg} ->
            {reply, Reply, NewState} = handle_call(Msg, From, State),
            {_, Reply} = gen:reply(From, Reply),
            loop(Parent, NewState);
        {'DOWN', _Mref, process, _Pid, _} ->
            NewState = forget_replica(State),
            loop(Parent, NewState);
        {system, From, Msg} ->
            sys:handle_system_msg(Msg, From, Parent, ?MODULE, [], State);
        Msg ->
            NewState = handle_proxy(Msg, State),
            loop(Parent, NewState)
    end.

handle_call(get_replica_pid, _From, State) ->
    case get_replica_pid(State) of
        {Pid, NewState} ->
            {reply, {ok, Pid}, NewState};
        not_started ->
            {reply, not_started, State}
    end;

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

%% TODO: check replica state periodically, tell manager forget it when the replica busy
handle_proxy(Msg = {'$gen_event', #replica_request{sender = Sender}}, State) ->
    case get_replica_pid(State) of
        {Pid, NewState} ->
            Pid ! Msg,
            NewState;
        not_started ->
            distributed_proxy_message:reply(Sender, {temporarily_unavailable, not_started}),
            State
    end;
handle_proxy(Msg, State) ->
    case get_replica_pid(State) of
        {Pid, NewState} ->
            Pid ! Msg,
            NewState;
        not_started ->
            State
    end.

get_replica_pid(State = #state{index=Index, replica_pid = undefined}) ->
    case distributed_proxy_replica_manager:get_replica_pid(Index) of
        {ok, Pid} ->
            Mref = erlang:monitor(process, Pid),
            {Pid, State#state{replica_pid = Pid, replica_monitor = Mref}};
        not_found ->
            not_started
    end;
get_replica_pid(State = #state{replica_pid = Pid}) ->
    {Pid, State}.

forget_replica(State) ->
    State#state{replica_pid = undefined, replica_monitor = undefined}.