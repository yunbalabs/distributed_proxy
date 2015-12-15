%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 3:42 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_replica_manager).
-author("zy").

-behaviour(gen_server).

%% API
-export([start_link/0, get_replica_pid/1, unregister_replica/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(ETS, ets_distributed_proxy_replica_manager).

-record(state, {
    idxtab       %% cache idx -> live_replica_pid
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_replica_pid(Idx) ->
    case ets:lookup(?ETS, Idx) of
        [{Idx, Pid}] ->
            {ok, Pid};
        [] ->
            not_found
    end.

unregister_replica(Idx, Pid) ->
    gen_server:call(?SERVER, {unregister_replica, Idx, Pid}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    ?ETS = ets:new(?ETS, [named_table, protected]),
    self() ! tick,
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
        State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({unregister_replica, Idx, Pid}, _From, State) ->
    case ets:lookup(?ETS, Idx) of
        [{Idx, Pid}] ->
            %% remove monitor when 'DOWN' received
            ets:delete(?ETS, Idx);
        [_] ->
            ignore;
        [] ->
            ignore
    end,
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(tick, State) ->
    schedule_management_timer(),
    {ok, Ring} = distributed_proxy_ring_manager:get_ring(),
    AllOwners = distributed_proxy_ring:get_owners(Ring),

    %% TODO: For stopping the replica, judge by the ring of data ownership have been transfered
    {ShouldStart, ShouldStop} = lists:partition(
        fun({_Idx, GroupId}) ->
            Nodes = distributed_proxy_ring:get_nodes(GroupId, Ring),
            lists:member(node(), Nodes)
        end, AllOwners),

    {ShouldStart2, _Started} = lists:partition(
        fun({Idx, _GroupId}) ->
            case ets:lookup(?ETS, Idx) of
                [{Idx, Pid}] ->
                    not is_process_alive(Pid);
                []  ->
                    true
            end
        end, ShouldStart),

    maybe_stop_replica(ShouldStop),
    maybe_start_replica(ShouldStart2, Ring),
    {noreply, State};
handle_info({'DOWN', MonRef, process, _P, _I}, State) ->
    case ets:lookup(?ETS, MonRef) of
        [{MonRef, Idx}] ->
            ets:delete(?ETS, Idx),
            ets:delete(?ETS, MonRef);
        [] ->
            lager:critical("Failed to find the monref")
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
        State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
        Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
schedule_management_timer() ->
    ManagementTick = distributed_proxy_config:check_replica_interval(),
    erlang:send_after(ManagementTick, ?MODULE, tick).

maybe_start_replica(StartIdx, Ring) ->
    StartFun =
        fun({Idx, GroupId}) ->
            Nodes = distributed_proxy_ring:get_nodes(GroupId, Ring),
            GroupIndex = distributed_proxy_util:index_of(node(), Nodes),
            lager:debug("Will start replica for partition ~p_~p", [Idx, GroupIndex]),
            {ok, Pid} =
                distributed_proxy_replica_sup:start_replica({Idx, GroupIndex}),
            lager:debug("Started replica, waiting for initialization to complete ~p, ~p_~p", [Pid, Idx, GroupIndex]),
            case distributed_proxy_replica:wait_for_init(Pid) of
                ok ->
                    lager:debug("Replica initialization ready ~p, ~p_~p", [Pid, Idx, GroupIndex]),
                    {Idx, Pid};
                Error ->
                    lager:error("Replica initialization failed ~p ~p_~p ~p", [Pid, Idx, GroupIndex, Error]),
                    {error, Error}
            end
        end,
    MaxStart = distributed_proxy_config:replica_parallel_start_count(),
    StartResult = distributed_proxy_util:pmap(StartFun, StartIdx, MaxStart),

    [begin
         case Result of
             {error, _} ->
                 try_again_later;
             {Idx, Pid} ->
                 MonRef = erlang:monitor(process, Pid),
                 ets:insert(?ETS, [{Idx, Pid}, {MonRef, Idx}])
         end
     end || Result <- StartResult].

maybe_stop_replica(StopIdx) ->
    [begin
         case ets:lookup(?ETS, Idx) of
             [{Idx, Pid}] ->
                 lager:debug("Will stop replica for partition ~p ~p", [Idx, Pid]),
                 distributed_proxy_replica:trigger_stop(Pid);
             [] ->
                 ok
         end
     end || {Idx, _GroupId} <- StopIdx].