%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 2:43 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_replica).
-author("zy").

-behaviour(gen_fsm).

%% API
-export([start_link/1, wait_for_init/1, get_state/1, refuse_request/1, accept_request/1]).

%% gen_fsm callbacks
-export([init/1,
    warn_up/2,
    started/3,
    active/2,
    refuse/2,
    handle_event/3,
    handle_sync_event/4,
    handle_info/3,
    terminate/3,
    code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {module, module_state, index, group_index, warn_up_check_interval, warn_up_timeout}).

-include("distributed_proxy_replica.hrl").

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Args :: term()) -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link(Args) ->
    gen_fsm:start_link(?MODULE, [Args], []).

wait_for_init(Pid) ->
    gen_fsm:sync_send_event(Pid, wait_for_init, infinity).

get_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, get_state).

refuse_request(Pid) ->
    gen_fsm:send_all_state_event(Pid, refuse_request).

accept_request(Pid) ->
    gen_fsm:send_all_state_event(Pid, accept_request).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, StateName :: atom(), StateData :: #state{}} |
    {ok, StateName :: atom(), StateData :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([{Idx, GroupIndex}]) ->
    process_flag(trap_exit, true),
    Module = distributed_proxy_config:replica_module(),
    WarnUpCheckInterval = distributed_proxy_config:warn_up_check_interval(),
    WarnUpTimeout = distributed_proxy_config:warn_up_timeout(),
    {ok, started, #state{
        index = Idx, group_index = GroupIndex,
        module = Module,
        warn_up_check_interval = WarnUpCheckInterval, warn_up_timeout = WarnUpTimeout}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @end
%%--------------------------------------------------------------------
-spec(warn_up(Event :: term(), State :: #state{}) ->
    {next_state, NextStateName :: atom(), NextState :: #state{}} |
    {next_state, NextStateName :: atom(), NextState :: #state{},
        timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
warn_up(check_tick, State = #state{warn_up_timeout = 0}) ->
    {stop, timeout, State};
warn_up(check_tick, State = #state{
    module = Module, module_state = ModuleState,
    warn_up_check_interval = CheckInterval, warn_up_timeout = Timeout,
    index = Index, group_index = GroupIndex
}) ->
    case Module:check_warnup_state(ModuleState) of
        {ok, up, ModuleState2} ->
            replica_actived(State),
            lager:info("replica ~p_~p state changed ~p -> ~p", [Index, GroupIndex, warn_up, active]),
            {next_state, active, State#state{module_state = ModuleState2}};
        {ok, _, ModuleState2} ->
            gen_fsm:send_event_after(CheckInterval, check_tick),
            {next_state, warn_up, State#state{module_state = ModuleState2, warn_up_timeout = Timeout - 1}};
        {error, Reason} ->
            {stop, Reason, State}
    end;
warn_up(#replica_request{sender = Sender}, State) ->
    distributed_proxy_message:reply(Sender, {temporarily_unavailable, warn_up}),
    {next_state, warn_up, State};
warn_up(Req, State) ->
    lager:error("unknown request ~p", [Req]),
    {next_state, warn_up, State}.

active(#replica_request{request = Request, sender = Sender}, State = #state{module = Module, module_state = ModuleState}) ->
    case Module:handle_request(Request, Sender, ModuleState) of
        {reply, Reply, ModuleState2} ->
            distributed_proxy_message:reply(Sender, Reply),
            {next_state, active, State#state{module_state = ModuleState2}};
        {noreply, ModuleState2} ->
            {next_state, active, State#state{module_state = ModuleState2}};
        {stop, Reason, ModuleState2} ->
            {stop, Reason, State#state{module_state = ModuleState2}}
    end;
active(Req, State) ->
    lager:error("unknown request ~p", [Req]),
    {next_state, active, State}.

refuse(#replica_request{sender = Sender}, State) ->
    distributed_proxy_message:reply(Sender, {error, refuse}),
    {next_state, refuse, State};
refuse(Req, State) ->
    lager:error("unknown request ~p", [Req]),
    {next_state, refuse, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(started(Event :: term(), From :: {pid(), term()},
        State :: #state{}) ->
    {next_state, NextStateName :: atom(), NextState :: #state{}} |
    {next_state, NextStateName :: atom(), NextState :: #state{},
        timeout() | hibernate} |
    {reply, Reply, NextStateName :: atom(), NextState :: #state{}} |
    {reply, Reply, NextStateName :: atom(), NextState :: #state{},
        timeout() | hibernate} |
    {stop, Reason :: normal | term(), NewState :: #state{}} |
    {stop, Reason :: normal | term(), Reply :: term(),
        NewState :: #state{}}).
started(wait_for_init, _From, State = #state{
    module = Module,
    warn_up_check_interval = CheckInterval,
    index = Index, group_index = GroupIndex
}) ->
    case Module:init(Index, GroupIndex) of
        {ok, ModuleState} ->
            lager:info("replica ~p_~p state changed ~p -> ~p", [Index, GroupIndex, started, warn_up]),
            gen_fsm:send_event_after(CheckInterval, check_tick),

            {reply, ok, warn_up, State#state{module_state = ModuleState}};
        {error, Reason} ->
            {stop, Reason, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_event(Event :: term(), StateName :: atom(),
        StateData :: #state{}) ->
    {next_state, NextStateName :: atom(), NewStateData :: #state{}} |
    {next_state, NextStateName :: atom(), NewStateData :: #state{},
        timeout() | hibernate} |
    {stop, Reason :: term(), NewStateData :: #state{}}).
handle_event(refuse_request, StateName, State = #state{index = Index, group_index = GroupIndex}) ->
    lager:info("replica ~p_~p state changed ~p -> ~p", [Index, GroupIndex, StateName, refuse]),
    {next_state, refuse, State};
handle_event(accept_request, StateName, State = #state{index = Index, group_index = GroupIndex}) ->
    lager:info("replica ~p_~p state changed ~p -> ~p", [Index, GroupIndex, StateName, active]),
    {next_state, active, State};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_sync_event(Event :: term(), From :: {pid(), Tag :: term()},
        StateName :: atom(), StateData :: term()) ->
    {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term()} |
    {reply, Reply :: term(), NextStateName :: atom(), NewStateData :: term(),
        timeout() | hibernate} |
    {next_state, NextStateName :: atom(), NewStateData :: term()} |
    {next_state, NextStateName :: atom(), NewStateData :: term(),
        timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewStateData :: term()} |
    {stop, Reason :: term(), NewStateData :: term()}).
handle_sync_event(get_state, _From, StateName, State) ->
    {reply, State#state.module_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), StateName :: atom(),
        StateData :: term()) ->
    {next_state, NextStateName :: atom(), NewStateData :: term()} |
    {next_state, NextStateName :: atom(), NewStateData :: term(),
        timeout() | hibernate} |
    {stop, Reason :: normal | term(), NewStateData :: term()}).
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: normal | shutdown | {shutdown, term()}
| term(), StateName :: atom(), StateData :: term()) -> term()).
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, StateName :: atom(),
        StateData :: #state{}, Extra :: term()) ->
    {ok, NextStateName :: atom(), NewStateData :: #state{}}).
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
replica_actived(_State) ->
    %% TODO: update the ring
    ok.