## shallow clone for speed

REBAR_GIT_CLONE_OPTIONS += --depth 1
export REBAR_GIT_CLONE_OPTIONS

REBAR = rebar3
all: compile

compile:
	$(REBAR) compile

ct: compile
	$(REBAR) as test ct -v

eunit: compile
	$(REBAR) as test eunit

xref:
	$(REBAR) xref

dialyzer:
	$(REBAR) dialyzer

clean: distclean

distclean:
	@rm -rf _build cover deps logs log data
	@rm -rf rebar.lock compile_commands.json
	@rm -rf rebar3.crashdump
