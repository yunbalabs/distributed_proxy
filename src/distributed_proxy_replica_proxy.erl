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
-export([start_link/1]).

%% callbacks
-export([init/1]).

-record(state, {
    replica_pid            %% need monitor
}).

start_link(Index) ->
    RegName = reg_name(Index),
    proc_lib:start_link(?MODULE, init, [[self(), RegName, Index]]).

init([Parent, RegName, _IndexBin]) ->
    erlang:register(RegName, self()),
    proc_lib:init_ack(Parent, {ok, self()}),

    State = #state{},
    loop(Parent, State).

%%%===================================================================
%%% Internal functions
%%%===================================================================
loop(Parent, State) ->
    receive
        _Msg ->
            loop(Parent, State)
    end.

reg_name(Index) ->
    AllBin = <<$d,$i,$s,$t,$r,$i,$b,$u,$t,$e,$d,$_,$p,$r,$o,$x,$y,$_,$r,$e,$p,$l,$i,$c,$a,$_,$p,$r,$o,$x,$y,$_, Index/binary>>,
    binary_to_atom(AllBin, latin1).