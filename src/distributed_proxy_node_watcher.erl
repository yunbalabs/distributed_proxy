%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 3:40 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_node_watcher).
-author("zy").

-behaviour(gen_server).

%% API
-export([start_link/0,nodes/0, is_up/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    bcast_tref = undefined,
    status = up
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

nodes() ->
    ets:tab2list(?MODULE).

is_up(Node) ->
    case ets:lookup(?MODULE, Node) of
        [{Node, _TimeStamp}] ->
            true;
        [] ->
            false
    end.

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
    %% Watch for node up/down events
    ok = net_kernel:monitor_nodes(true),

    %% Setup ETS table to track node status
    ?MODULE = ets:new(?MODULE, [protected, {read_concurrency, true}, named_table]),

    Now = distributed_proxy_util:moment(),
    ets:insert(?MODULE, [{Node, Now} || Node <- [erlang:node() | erlang:nodes()]]),

    {ok, schedule_broadcast(#state{})}.

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
handle_cast({up, Node}, State) ->
    State2 = node_up(Node, State),
    {noreply, State2};

handle_cast({down, Node}, State) ->
    State2 = node_down(Node, State),
    {noreply, State2};

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
handle_info({nodeup, _Node}, State) ->
    {noreply, State};
handle_info({nodedown, Node}, State) ->
    State2 = node_down(Node, State),
    {noreply, State2};
handle_info(broadcast, State) ->
    {ok, MyRing} = distributed_proxy_ring_manager:get_ring(),
    AllNodes = distributed_proxy_ring:get_all_nodes(MyRing),
    State2 = broadcast(AllNodes, State),
    {noreply, State2};
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
terminate(_Reason, State) ->
    case distributed_proxy_ring_manager:get_ring() of
        {ok, MyRing} ->
            AllNodes = distributed_proxy_ring:get_all_nodes(MyRing),
            broadcast(AllNodes, State#state { status = down });
        {error, _} ->
            ignore
    end,
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
node_up(Node, State) ->
    S2 = case is_up(Node) of
             false ->
                 lager:info("Node ~p up", [Node]),
                 broadcast([Node], State);
             true ->
                 State
         end,

    node_update(Node, S2).

node_update(Node, State) ->
    Now = distributed_proxy_util:moment(),
    ets:insert(?MODULE, {Node, Now}),
    State.

node_down(Node, State) ->
    lager:info("Node ~p down", [Node]),

    ets:delete(?MODULE, Node),
    %% TODO: shutdown when lost connections with more than half of nodes
    State.

schedule_broadcast(State) ->
    case (State#state.bcast_tref) of
        undefined ->
            ok;
        OldTref ->
            _ = erlang:cancel_timer(OldTref),
            ok
    end,
    Interval = distributed_proxy_config:broadcast_interval(),
    Tref = erlang:send_after(Interval, self(), broadcast),
    State#state{bcast_tref = Tref}.

broadcast(Nodes, State) ->
    case (State#state.status) of
        up ->
            Msg = {up, node()};
        down ->
            Msg = {down, node()}
    end,
    gen_server:abcast(Nodes, ?MODULE, Msg),
    schedule_broadcast(State).