-module(bloom_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    ok = bloom_stats:init(),
	bloom_sup:start_link().

stop(_State) ->
	ok.
