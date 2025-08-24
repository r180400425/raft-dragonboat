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

%% @doc 该模块实现了无等待查询处理的 API 功能，遵循 eh_query_handler 行为规范。
%% 主要用于处理查询请求，当检测到对象正在更新时，向客户端返回错误信息。
%% @end
-module(eh_no_wait_query_handler_api).

%% 声明该模块遵循 eh_query_handler 行为，需要实现该行为定义的回调函数
-behavior(eh_query_handler).

%% 导出 process 函数，供外部模块调用
-export([process/6]).

%% @doc 处理查询请求。当对象正在更新时，向请求发起者返回错误信息。
%% @spec process(ObjectType :: term(), ObjectId :: term(), NodeId :: term(), From :: pid(), Ref :: reference(), State :: term()) -> term().
%% @param ObjectType 表示要查询的对象类型。
%% @param ObjectId 表示要查询的对象唯一标识符。
%% @param _NodeId 表示当前节点的标识符，该参数在本函数中未使用。
%% @param From 通常是发起请求的进程的标识符，用于返回响应。
%% @param Ref 是一个引用，用于标识特定的请求，以便在响应时能够正确匹配。
%% @param State 是系统的状态信息。
%% @return 返回更新后的系统状态。
%% @end
process(ObjectType, ObjectId, _NodeId, From, Ref, State) ->
  %% 调用 eh_query_handler 模块的 error_being_updated 函数生成对象正在更新的错误信息，
  %% 再调用 eh_query_handler 模块的 reply 函数将错误信息返回给请求发起者
  eh_query_handler:reply(From, Ref, eh_query_handler:error_being_updated([{ObjectType, ObjectId, undefined}])),
  %% 返回原始的系统状态，因为本函数未对状态进行修改
  State.


