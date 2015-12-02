REBAR = ./rebar -j8

.PHONY: deps

all: deps compile

deps:
	${REBAR} get-deps

compile: deps
	${REBAR} compile