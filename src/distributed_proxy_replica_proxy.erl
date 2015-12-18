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
-export([
    start_link/3, get_my_replica_pid/1, forget_my_replica/1, call/2, call/3, cast/2,
    system_continue/3, system_terminate/4, system_code_change/4
]).

%% callbacks
-export([init/1]).

-record(state, {
    index, group_index,
    replica_pid = undefined,
    replica_monitor,
    check_counter, ping_state, check_ping_interval, check_interval, check_overload_threshold
}).

-include("distributed_proxy_replica.hrl").

start_link(IndexName, Idx, GroupIndex) ->
    proc_lib:start_link(?MODULE, init, [[self(), IndexName, Idx, GroupIndex]]).

get_my_replica_pid(ProxyName) ->
    call(ProxyName, get_replica_pid).

forget_my_replica(ProxyName) ->
    call(ProxyName, forget_replica).

call(Name, Msg) ->
    call_reply(catch gen:call(Name, '$replica_proxy_call', Msg)).

call(Name, Msg, Timeout) ->
    call_reply(catch gen:call(Name, '$replica_proxy_call', Msg, Timeout)).

cast(Name, Msg) ->
    catch erlang:send(Name, {'$replica_proxy_cast', Msg}),
    ok.

system_continue(Parent, _, State) ->
    loop(Parent, State).

system_terminate(Reason, _Parent, _, _State) ->
    exit(Reason).

system_code_change(State, _, _, _) ->
    {ok, State}.

-spec call_reply({atom(), term()}) -> term().
call_reply({ok, Res}) ->
    Res;
call_reply({'EXIT', Reason}) ->
    {error, Reason}.

init([Parent, IndexName, Idx, GroupIndex]) ->
    RegName = distributed_proxy_util:replica_proxy_reg_name(IndexName),
    erlang:register(RegName, self()),
    proc_lib:init_ack(Parent, {ok, self()}),

    loop(Parent, #state{
        index = Idx, group_index = GroupIndex,
        check_counter = 0, ping_state = undefined,
        check_ping_interval = distributed_proxy_config:replica_proxy_ping_interval(),
        check_interval = distributed_proxy_config:replica_proxy_check_interval(),
        check_overload_threshold = distributed_proxy_config:replica_proxy_overload_threshold()
    }).

%%%===================================================================
%%% Internal functions
%%%===================================================================
loop(Parent, State) ->
    receive
        {'$replica_proxy_call', From, Msg} ->
            {reply, Reply, NewState} = handle_call(Msg, From, State),
            {_, Reply} = gen:reply(From, Reply),
            loop(Parent, NewState);
        {'$replica_proxy_cast', Msg} ->
            {noreply, NewState} = handle_cast(Msg, State),
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

handle_call(forget_replica, _From, State) ->
    NewState = forget_replica(State),
    {reply, ok, NewState};

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({replica_proxy_pong, Pid}, State=#state{
    replica_pid = ReplicaPid, ping_state = PingState
}) ->
    ValidReply = (Pid =:= ReplicaPid) and (PingState =:= sent),
    NewState = case ValidReply of
                   true ->
                       %% TODO: only subtracting mailbox length before ping
                       State#state{ping_state = undefined, check_counter = 0};
                   _ ->
                       State#state{ping_state = undefined}
               end,
    {noreply, NewState}.

handle_proxy(Msg = {'$gen_event', #replica_request{sender = Sender}}, State) ->
    case get_replica_pid(State) of
        {Pid, NewState} ->
            case overload_threshold_check(Pid, NewState) of
                {ok, NewState2} ->
                    Pid ! Msg,
                    NewState2;
                {overload, NewState2} ->
                    NewState2
            end;
        not_started ->
            %% TODO: if the replica isn't in the ring, forward the msg to the right proxy
            distributed_proxy_message:reply(Sender, {temporarily_unavailable, not_started}),
            State
    end;
handle_proxy(Msg, State) ->
    case get_replica_pid(State) of
        {Pid, NewState} ->
            case overload_threshold_check(Pid, NewState) of
                {ok, NewState2} ->
                    Pid ! Msg,
                    NewState2;
                {overload, NewState2} ->
                    NewState2
            end;
        not_started ->
            State
    end.

get_replica_pid(State = #state{index = Index, group_index = GroupIndex, replica_pid = undefined}) ->
    case distributed_proxy_replica_manager:get_replica_pid({Index, GroupIndex}) of
        {ok, Pid} ->
            Mref = erlang:monitor(process, Pid),
            {Pid, State#state{replica_pid = Pid, replica_monitor = Mref}};
        not_found ->
            not_started
    end;
get_replica_pid(State = #state{replica_pid = Pid}) ->
    {Pid, State}.

forget_replica(State) ->
    State#state{replica_pid = undefined, replica_monitor = undefined, check_counter = 0, ping_state = undefined}.

overload_threshold_check(ReplicaPid, State = #state{
    index = Idx, group_index = GroupIndex,
    check_counter = Counter, ping_state = PingState,
    check_ping_interval = PingInterval, check_interval = Interval, check_overload_threshold = OverloadThreshold
}) ->
    Counter2 = Counter + 1,
    {NewCounter, NewPingState, MailboxLen} = case Counter2 of
        PingInterval ->
            %% Ping the replica in hopes that we get a pong back before hitting
            %% the hard query interval and triggering an expensive process_info
            %% call. A successful pong from the replica means that all messages
            %% sent before the ping have already been handled and therefore
            %% we can adjust our mailbox estimate accordingly.
            case PingState of
                undefined ->
                    ReplicaPid ! {'$replica_proxy_ping', self()},
                    PingState2 = sent;
                _ ->
                    PingState2 = PingState
            end,
            {Counter2, PingState2, Counter2};
        Interval ->
            %% Time to directly check the mailbox size. This operation may
            %% be extremely expensive. If the replica is currently active,
            %% the proxy will be descheduled until the replica finishes
            %% execution and becomes descheduled itself.
            case erlang:process_info(ReplicaPid, message_queue_len) of
                undefined ->
                    {0, undefined, OverloadThreshold};
                {_, L} ->
                    PingState2 = case PingState of
                                     sent ->
                                         %% Ignore pending ping response as it is
                                         %% no longer valid nor useful.
                                         ignore;
                                     _ ->
                                         PingState
                                 end,
                    {0, PingState2, L}
            end;
        _ ->
            {Counter2, PingState, Counter2}
    end,

    case MailboxLen < OverloadThreshold of
        true ->
            {ok, State#state{check_counter = NewCounter, ping_state = NewPingState}};
        false ->
            NewState = forget_replica(State),
            distributed_proxy_replica_manager:unregister_replica({Idx, GroupIndex}, ReplicaPid),
            distributed_proxy_replica:trigger_stop(ReplicaPid),
            {overload, NewState}
    end.