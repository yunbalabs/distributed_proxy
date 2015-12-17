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
-export([start_link/0, add_node/1, set_ring/1, set_ring/2, get_ring/0]).

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

set_ring(Ring) ->
    set_ring(node(), Ring).

set_ring(Node, Ring) ->
    gen_server:call({?SERVER, Node}, {set_ring, Ring}, infinity).

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
            join_cluster(lists:delete(node(), AllNodes)),
            cache_ring(Ring),
            {ok, #state{ring = Ring}}
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
                    do_write_ringfile(Ring2),
                    cache_ring(Ring2),

                    %% TODO: should use gossip to make the ring consistency on all nodes
                    AllNodes = distributed_proxy_ring:get_all_nodes(Ring2),
                    lists:foreach(
                        fun(EachNode) ->
                            ok = set_ring(EachNode, Ring2)
                        end,
                        lists:delete(node(), AllNodes)),

                    {reply, ok, State#state{ring = Ring2}};
                {still_reconciling, Ring2} ->
                    {reply, still_reconciling, State#state{ring = Ring2}}
            end
    end;
handle_call({set_ring, NewRing}, _From, State) ->
    do_write_ringfile(NewRing),
    cache_ring(NewRing),
    {reply, ok, State#state{ring = NewRing}};
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
    {noreply,State};

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