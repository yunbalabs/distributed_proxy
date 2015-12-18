%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 02. 十二月 2015 12:08 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy_replica_example).
-author("zy").

%% API
-export([init/2, check_warnup_state/1, actived/1, handle_request/3, terminate/2, get_slaveof_state/1]).

-record(state, {}).

init(_Index, _GroupIndex) ->
    {ok, #state{}}.

check_warnup_state(State) ->
    {ok, up, State}.

actived(State) ->
    {ok, State}.

handle_request(Request, Sender, ModuleState) ->
    lager:info("receive the request ~p from ~p", [Request, Sender]),
    {reply, example, ModuleState}.

terminate(_Reason, _State) ->
    ok.

get_slaveof_state(_State) ->
    {ok, finished}.