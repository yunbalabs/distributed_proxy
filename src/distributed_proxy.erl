%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 30. 十一月 2015 3:47 PM
%%%-------------------------------------------------------------------
-module(distributed_proxy).
-author("zy").

%% API
-export([start/0, stop/0]).

start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).