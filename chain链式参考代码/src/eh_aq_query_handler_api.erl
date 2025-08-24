%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copyright (c) 2016 Gyanendra Aggarwal.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


% eh_aq_query_handler_api：Erlang 应用的查询处理 API 模块，定义了处理查询请求的接口。
% eh_aq_query_handler_api 模块实现了 eh_query_handler 行为的 process 函数，该函数的主要功能是计算出有效的尾节点标识符，并将查询处理逻辑委托给 eh_query_handler 模块的 process_tail 函数。

-module(eh_aq_query_handler_api).
% 这行代码定义了一个名为 eh_aq_query_handler_api 的 Erlang 模块。在 Erlang 中，每个文件通常只包含一个模块，模块名必须与文件名（不包括 .erl 扩展名）一致。
-behavior(eh_query_handler).
% 这行代码声明该模块遵循 eh_query_handler 行为（behavior）。行为类似于其他编程语言中的接口，它定义了一组函数签名，实现该行为的模块必须实现这些函数。这有助于代码的规范和可维护性，确保模块具有预期的功能。
-export([process/6]).
% 这行代码将 process 函数导出，使其可以被其他模块调用。process/6 中的 6 表示该函数接受 6 个参数。在 Erlang 中，函数的标识不仅取决于函数名，还取决于参数的数量（也称为函数的 arity）。
-include("erlang_craq.hrl").
% 这行代码包含了名为 erlang_craq.hrl 的头文件。头文件通常用于定义宏、记录（records）等，这些定义可以在包含该头文件的模块中直接使用。
process(ObjectType, ObjectId, NodeId, From, Ref, #eh_system_state{repl_ring_order=ReplRingOrder, repl_ring=ReplRing, app_config=AppConfig}=State) ->
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  % 这行代码调用 eh_system_config 模块的 get_node_order 函数，传入 AppConfig 作为参数，获取节点的排序方式，并将结果赋值给 NodeOrder 变量。
  Tail = eh_repl_ring:effective_tail_node_id(NodeId, ReplRing, ReplRingOrder, NodeOrder),
  % 计算出有效的尾节点标识符，并将结果赋值给 Tail 变量。
  eh_query_handler:process_tail(ObjectType, ObjectId, Tail, From, Ref, State).
  % 处理尾节点的查询请求。这表明该模块将具体的查询处理逻辑委托给了 eh_query_handler 模块的 process_tail 函数。

% 这行代码定义了 process 函数，它接受 6 个参数：
% ObjectType：表示对象的类型。
% ObjectId：表示对象的唯一标识符。
% NodeId：表示当前节点的标识符。
% From：通常是发起请求的进程的标识符，用于返回响应。
% Ref：是一个引用，用于标识特定的请求，以便在响应时能够正确匹配。
% State：是一个 #eh_system_state 记录，包含了系统的状态信息。通过模式匹配，将 State 中的 repl_ring_order、repl_ring 和 app_config 分别绑定到 ReplRingOrder、ReplRing 和 AppConfig 变量上。




