%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. 十二月 2015 10:48 AM
%%%-------------------------------------------------------------------
-module(distributed_proxy_message).
-author("zy").

%% API
-export([send/2, reply/2, recv/0]).

-define(DEFAULT_RECV_TIMEOUT, 5000). %% 5s

-include("distributed_proxy_replica.hrl").

send(Dest, Msg) ->
    gen_fsm:send_event(Dest, #replica_request{request = Msg, sender = self()}).

reply(Sender, Msg) ->
    catch erlang:send(Sender, Msg, [noconnect, nosuspend]).

recv() ->
    recv(?DEFAULT_RECV_TIMEOUT).

recv(Timeout) ->
    receive Msg -> Msg
    after Timeout -> {error, timeout}
    end.