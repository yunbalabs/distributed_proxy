%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 2:12 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_util).
-author("zy").

%% API
-export([start_app_deps/1, replace_file/2, moment/0, index_of/2, pmap/3, replica_proxy_reg_name/1, safe_rpc/5]).

%% 719528 days from Jan 1, 0 to Jan 1, 1970
%%  *86400 seconds/day
-define(SEC_TO_EPOCH, 62167219200).

%% @spec start_app_deps(App :: atom()) -> ok
%% @doc Start depedent applications of App.
start_app_deps(App) ->
    {ok, DepApps} = application:get_key(App, applications),
    _ = [ensure_started(A) || A <- DepApps],
    ok.

%% @spec ensure_started(Application :: atom()) -> ok
%% @doc Start the named application if not already started.
ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

%% @doc Atomically/safely (to some reasonable level of durablity)
%% replace file `FN' with `Data'. NOTE: since 2.0.3 semantic changed
%% slightly: If `FN' cannot be opened, will not error with a
%% `badmatch', as before, but will instead return `{error, Reason}'
-spec replace_file(string(), iodata()) -> ok | {error, term()}.
replace_file(FN, Data) ->
    TmpFN = FN ++ ".tmp",
    case file:open(TmpFN, [write, raw]) of
        {ok, FH} ->
            try
                ok = file:write(FH, Data),
                ok = file:sync(FH),
                ok = file:close(FH),
                ok = file:rename(TmpFN, FN),
                {ok, Contents} = read_file(FN),
                true = (Contents == iolist_to_binary(Data)),
                ok
            catch _:Err ->
                {error, Err}
            end;
        Err ->
            Err
    end.

%% @doc Similar to {@link file:read_file/1} but uses raw file `I/O'
read_file(FName) ->
    {ok, FD} = file:open(FName, [read, raw, binary]),
    IOList = read_file(FD, []),
    ok = file:close(FD),
    {ok, iolist_to_binary(IOList)}.

read_file(FD, Acc) ->
    case file:read(FD, 4096) of
        {ok, Data} ->
            read_file(FD, [Data|Acc]);
        eof ->
            lists:reverse(Acc)
    end.

-record(pmap_acc,{
    mapper,
    fn,
    n_pending=0,
    pending=sets:new(),
    n_done=0,
    done=[],
    max_concurrent=1
}).

%% @doc Parallel map with a cap on the number of concurrent worker processes.
%% Note: Worker processes are linked to the parent, so a crash propagates.
-spec pmap(Fun::function(), List::list(), MaxP::integer()) -> list().
pmap(Fun, List, MaxP) when MaxP < 1 ->
    pmap(Fun, List, 1);
pmap(Fun, List, MaxP) when is_function(Fun), is_list(List), is_integer(MaxP) ->
    Mapper = self(),
    #pmap_acc{pending=Pending, done=Done} =
        lists:foldl(fun pmap_worker/2,
            #pmap_acc{mapper=Mapper,
                fn=Fun,
                max_concurrent=MaxP},
            List),
    All = pmap_collect_rest(Pending, Done),
    % Restore input order
    Sorted = lists:keysort(1, All),
    [ R || {_, R} <- Sorted ].

%% @doc Fold function for {@link pmap/3} that spawns up to a max number of
%% workers to execute the mapping function over the input list.
pmap_worker(X, Acc = #pmap_acc{n_pending=NP,
    pending=Pending,
    n_done=ND,
    max_concurrent=MaxP,
    mapper=Mapper,
    fn=Fn})
    when NP < MaxP ->
    Worker =
        spawn_link(fun() ->
            R = Fn(X),
            Mapper ! {pmap_result, self(), {NP+ND, R}}
                   end),
    Acc#pmap_acc{n_pending=NP+1, pending=sets:add_element(Worker, Pending)};
pmap_worker(X, Acc = #pmap_acc{n_pending=NP,
    pending=Pending,
    n_done=ND,
    done=Done,
    max_concurrent=MaxP})
    when NP == MaxP ->
    {Result, NewPending} = pmap_collect_one(Pending),
    pmap_worker(X, Acc#pmap_acc{n_pending=NP-1, pending=NewPending,
        n_done=ND+1, done=[Result|Done]}).

%% @doc Waits for one pending pmap task to finish
pmap_collect_one(Pending) ->
    receive
        {pmap_result, Pid, Result} ->
            Size = sets:size(Pending),
            NewPending = sets:del_element(Pid, Pending),
            case sets:size(NewPending) of
                Size ->
                    pmap_collect_one(Pending);
                _ ->
                    {Result, NewPending}
            end
    end.

pmap_collect_rest(Pending, Done) ->
    case sets:size(Pending) of
        0 ->
            Done;
        _ ->
            {Result, NewPending} = pmap_collect_one(Pending),
            pmap_collect_rest(NewPending, [Result | Done])
    end.

%% @spec moment() -> integer()
%% @doc Get the current "moment".  Current implementation is the
%%      number of seconds from year 0 to now, universal time, in
%%      the gregorian calendar.

moment() ->
    {Mega, Sec, _Micro} = os:timestamp(),
    (Mega * 1000000) + Sec + ?SEC_TO_EPOCH.

%% @doc Wraps an rpc:call/5 in a try/catch to handle the case where the
%%      'rex' process is not running on the remote node. This is safe in
%%      the sense that it won't crash the calling process if the rex
%%      process is down.
-spec safe_rpc(Node :: node(), Module :: atom(), Function :: atom(), Args :: [any()], Timeout :: timeout()) -> {'badrpc', any()} | any().
safe_rpc(Node, Module, Function, Args, Timeout) ->
    try rpc:call(Node, Module, Function, Args, Timeout) of
        Result ->
            Result
    catch
        'EXIT':{noproc, _NoProcDetails} ->
            {badrpc, rpc_process_down}
    end.

index_of(Item, List) -> index_of(Item, List, 1).

index_of(_, [], _)  -> not_found;
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index+1).

replica_proxy_reg_name(Index) ->
    AllBin = <<$d,$i,$s,$t,$r,$i,$b,$u,$t,$e,$d,$_,$p,$r,$o,$x,$y,$_,$r,$e,$p,$l,$i,$c,$a,$_,$p,$r,$o,$x,$y,$_, Index/binary>>,
    binary_to_atom(AllBin, latin1).