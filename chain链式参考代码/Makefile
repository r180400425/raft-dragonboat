.PHONY: all compile deps clean distclean test check_plt build_plt dialyzer cleanplt

all: deps compile

compile: deps
	 ./rebar compile

deps:
	test -d deps || ./rebar get-deps

clean:
	./rebar clean

distclean: clean
	   ./rebar delete-deps

DIALYZER_APPS = kernel stdlib erts sasl eunit syntax_tools compiler crypto common_test

# Makefile：用于自动化构建和管理项目。定义了一系列的目标，如 all、compile、deps、clean 等，通过执行 make 命令可以方便地进行项目的编译、依赖管理和清理工作。