%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 2:48 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_ring_manager).
-author("zy").

-behaviour(gen_server).

%% API
-export([start_link/0, add_node/1, set_ring/1, set_ring/2, complete_change/2, get_ring/0, fix_ring/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(ETS, ets_distributed_proxy_ring_manager).
-define(DEFAULT_RING_DIR, "data/ring").

-record(state, {ring}).

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

add_node(Node) ->
    gen_server:call(?SERVER, {add_node, Node}, infinity).

fix_ring() ->
    gen_server:call(?SERVER, fix_ring, infinity).

set_ring(Ring) ->
    set_ring(node(), Ring).

set_ring(Node, Ring) ->
    case catch gen_server:call({?SERVER, Node}, {set_ring, Ring}) of
        ok ->
            ok;
        {'EXIT', Reason} ->
            {error, Reason}
    end.

complete_change(Node, {Idx, GroupIndex, NewNode}) ->
    case catch gen_server:call({?SERVER, Node}, {complete_change, {Idx, GroupIndex, NewNode}}) of
        ok ->
            ok;
        {'EXIT', Reason} ->
            {error, Reason}
    end.

get_ring() ->
    try
        Ring = ets:lookup_element(?ETS, ring, 2),
        {ok, Ring}
    catch
        Error ->
            {error, Error}
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
    setup_ets(),
    case reload_ring() of
        {error, Reason} ->
            {stop, Reason};
        Ring ->
            AllNodes = distributed_proxy_ring:get_all_nodes(Ring),
            OtherNodes = lists:delete(node(), AllNodes),
            join_cluster(OtherNodes),
            Ring2 = fix_ring(OtherNodes, Ring),
            case Ring2 of
                Ring ->
                    cache_ring(Ring2),
                    {ok, #state{ring = Ring2}};
                _ ->
                    lager:info("the ring has been fixed"),
                    update_ring(Ring2, #state{ring = Ring})
            end
    end.

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
handle_call({add_node, Node}, _From, State = #state{ring = Ring}) ->
    case lists:member(Node, distributed_proxy_ring:get_all_nodes(Ring)) of
        true ->
            {reply, ok, State};
        false ->
            case distributed_proxy_ring:add_node(Node, Ring) of
                {ok, Ring2} ->
                    {ok, NewState} = update_ring(Ring2, State),

                    %% TODO: should use gossip to make the ring consistency on all nodes
                    AllNodes = distributed_proxy_ring:get_all_nodes(Ring2),
                    lists:foreach(
                        fun(EachNode) ->
                            case set_ring(EachNode, Ring2) of
                                ok ->
                                    ok;
                                {error, Reason} ->
                                    lager:error("update the ring failed ~p on ~p", [Reason, EachNode])
                            end
                        end,
                        lists:delete(node(), AllNodes)),

                    {reply, ok, NewState};
                {still_reconciling, Ring2} ->
                    {reply, still_reconciling, State#state{ring = Ring2}}
            end
    end;

handle_call({set_ring, NewRing}, _From, State) ->
    {ok, NewState} = update_ring(NewRing, State),
    {reply, ok, NewState};

handle_call({complete_change, {Idx, GroupIndex, NewNode}}, _From, State = #state{ring = Ring}) ->
    Ring2 = distributed_proxy_ring:complete_change(NewNode, {Idx, GroupIndex}, Ring),
    {ok, NewState} = update_ring(Ring2, State),
    {reply, ok, NewState};

handle_call(fix_ring, _From, State=#state{ring = Ring}) ->
    AllNodes = distributed_proxy_ring:get_all_nodes(Ring),
    OtherNodes = lists:delete(node(), AllNodes),
    Ring2 = fix_ring(OtherNodes, Ring),
    case Ring2 of
        Ring ->
            {reply, ok, State};
        _ ->
            lager:info("the ring has been fixed"),
            {ok, NewState} = update_ring(Ring2, State),
            {reply, ok, NewState}
    end;

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
handle_cast(write_ringfile, State=#state{ring=Ring}) ->
    ok = do_write_ringfile(Ring),
    {noreply, State};

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
setup_ets() ->
    ?ETS = ets:new(?ETS, [named_table, protected, {read_concurrency, true}]),
    ok.

reload_ring() ->
    case find_latest_ringfile() of
        {ok, RingFile} ->
            lager:debug("Find the ring file: ~p", [RingFile]),
            case read_ringfile(RingFile) of
                {error, Reason} ->
                    lager:critical("Failed to read ring file: ~p",
                        [lager:posix_error(Reason)]),
                    {error, Reason};
                Ring ->
                    Ring
            end;
        {error, not_found} ->
            distributed_proxy_ring:create();
        {error, enoent} ->
            distributed_proxy_ring:create();
        {error, Reason} ->
            lager:critical("Failed to load ring file: ~p",
                [lager:posix_error(Reason)]),
            {error, Reason}
    end.

update_ring(NewRing, State = #state{ring = OldRing}) ->
    do_write_ringfile(NewRing),
    cache_ring(NewRing),
    OldMap = distributed_proxy_ring:get_map(OldRing),
    NewMap = distributed_proxy_ring:get_map(NewRing),
    shutdown_unnecessary_replicas(OldMap, NewMap, OldRing),
    {ok, State#state{ring = NewRing}}.

cache_ring(Ring) ->
    Actions = [
        {ring, Ring}
    ],
    ets:insert(?ETS, Actions),
    ok.

find_latest_ringfile() ->
    Dir = ?DEFAULT_RING_DIR,
    case file:list_dir(Dir) of
        {ok, Filenames} ->
            Timestamps = [list_to_integer(TS) || {"ring", TS} <-
                [list_to_tuple(string:tokens(FN, ".")) || FN <- Filenames]],
            SortedTimestamps = lists:reverse(lists:sort(Timestamps)),
            case SortedTimestamps of
                [Latest | _] ->
                    {ok, Dir ++ "/ring." ++ integer_to_list(Latest)};
                _ ->
                    {error, not_found}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

read_ringfile(RingFile) ->
    case file:read_file(RingFile) of
        {ok, Binary} ->
            binary_to_term(Binary);
        {error, Reason} ->
            {error, Reason}
    end.

do_write_ringfile(Ring) ->
    Dir = ?DEFAULT_RING_DIR,

    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:universal_time(),
    TS = io_lib:format("~B~2.10.0B~2.10.0B~2.10.0B~2.10.0B~2.10.0B",
        [Year, Month, Day, Hour, Minute, Second]),
    FN = Dir ++ "/ring." ++ TS,
    do_write_ringfile(Ring, FN).

do_write_ringfile(Ring, FN) ->
    ok = filelib:ensure_dir(FN),
    try
        ok = distributed_proxy_util:replace_file(FN, term_to_binary(Ring))
    catch
        _:Err ->
            lager:error("Unable to write ring to \"~s\" - ~p\n", [FN, Err]),
            {error,Err}
    end.

join_cluster([]) ->
    lager:info("The cluster haven't any node");
join_cluster([Node|T]) ->
    case net_adm:ping(Node) of
        pong ->
            lager:info("Join the cluster by ~p", [Node]),
            ok;
        _ ->
            join_cluster(T)
    end.

shutdown_unnecessary_replicas([], [], _Ring) ->
    ok;
shutdown_unnecessary_replicas([{Pos, OldNodes} | OldMap], [{Pos, NewNodes} | NewMap], Ring) ->
    case OldNodes =:= NewNodes of
        true ->
            shutdown_unnecessary_replicas(OldMap, NewMap, Ring);
        false ->
            shutdown_unnecessary_replica(OldNodes, NewNodes, Pos, 1, Ring),
            shutdown_unnecessary_replicas(OldMap, NewMap, Ring)
    end.

shutdown_unnecessary_replica([], [], _, _, _) ->
    ok;
shutdown_unnecessary_replica([Same | OldNodes], [Same | NewNodes], Pos, GroupIndex, Ring) ->
    shutdown_unnecessary_replica(OldNodes, NewNodes, Pos, GroupIndex + 1, Ring);
shutdown_unnecessary_replica([Node | OldNodes], [_ | NewNodes], Pos, GroupIndex, Ring) ->
    case node() of
        Node ->
            {Idx, _} = distributed_proxy_ring:pos2index(Pos, Ring),
            ProxyName = distributed_proxy_util:replica_proxy_reg_name(list_to_binary(lists:flatten(io_lib:format("~w_~w", [Idx, GroupIndex])))),
            case distributed_proxy_replica_proxy:get_my_replica_pid(ProxyName) of
                {ok, Pid} ->
                    distributed_proxy_replica_manager:unregister_replica({Idx, GroupIndex}, Pid),
                    distributed_proxy_replica_proxy:forget_my_replica(ProxyName),
                    distributed_proxy_replica:trigger_stop(Pid);
                not_started ->
                    ignore
            end;
        _ ->
            ignore
    end,
    shutdown_unnecessary_replica(OldNodes, NewNodes, Pos, GroupIndex + 1, Ring).

fix_ring([], Ring) ->
    Ring;
fix_ring([Node | Else], Ring) ->
    Ring2 =
        case lists:member(Node, nodes()) of
            true ->
                case distributed_proxy_util:safe_rpc(Node, distributed_proxy_ring_manager, get_ring, [], 10000) of
                    {ok, NodeRing} ->
                        distributed_proxy_ring:merge_ring(Ring, NodeRing);
                    _ ->
                        Ring
                end;
            false ->
                Ring
        end,
    fix_ring(Else, Ring2).