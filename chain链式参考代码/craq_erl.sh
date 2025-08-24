#!/bin/bash

cd /Users/gyanendraaggarwal/erlang/code/erlang_craq

erl -sname $1 -pa ./ebin -pa ./craq_test/ebin -pa ./deps/lager/ebin -pa ./deps/goldrush/ebin -config ./sys

# craq_erl.sh：一个 shell 脚本，用于启动 Erlang 节点。脚本会进入项目目录，并使用指定的节点名启动 Erlang 虚拟机，同时加载项目的代码路径和配置文件。