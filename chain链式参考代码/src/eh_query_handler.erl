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


% 定义了 eh_query_handler 模块的导出函数、回调函数规范，以及一系列用于处理查询、错误信息和消息回复的函数。 process_pending 函数用于处理挂起的查询请求。


-module(eh_query_handler).

%% 导出模块对外提供的函数及其参数个数，方便其他模块调用
-export([reply/3,
         error_node_unavailable/1,
         error_node_down/1,
         error_being_updated/1,
         updated/1,
         query/6,
         process_pending/2,
         process_tail/6]).

%% 包含名为 erlang_craq.hrl 的头文件，该文件可能定义了宏、记录等
-include("erlang_craq.hrl").

%% 定义回调函数的规范，实现该行为的模块需要实现此函数
%% ObjectType 为原子类型，ObjectId 为任意类型，NodeId 为原子类型
%% From 为进程 ID，Ref 为任意类型，State 为 #eh_system_state{} 记录类型
%% 函数返回值为 #eh_system_state{} 记录类型
-callback process(ObjectType :: atom(), ObjectId :: term(), NodeId :: atom(), From :: pid(), Ref :: term(), State :: #eh_system_state{}) -> #eh_system_state{}.

%% reply 函数用于向指定进程发送回复消息
%% From 是接收消息的进程 ID，Ref 是消息引用，Reply 是具体的回复内容
reply(From, Ref, Reply) ->
  From ! {reply, Ref, Reply}.  
% reply 是函数名，在 Erlang 里，函数名需为小写字母开头的原子（atom）。
% (From, Ref, Reply) 是函数的参数列表，这表明 reply 函数接收三个参数：
% From：一般代表接收消息的进程的进程 ID（pid() 类型）。在 Erlang 进程间通信中，进程 ID 用于唯一标识一个进程。
% Ref：通常是消息引用，一般为一个唯一的标识符，可用于匹配请求和响应，类型为任意 Erlang 项（term()）。
% Reply：是要发送的具体回复内容，类型同样为任意 Erlang 项（term()）。
% -> 是 Erlang 里用于分隔函数头和函数体的符号。

% ! 是 Erlang 中的消息发送操作符。其语法为 Pid ! Message，意思是把 Message 消息发送给 Pid 所代表的进程。
% {reply, Ref, Reply} 是要发送的消息内容，它是一个元组（tuple）。元组在 Erlang 中常用来把多个相关的数据组合成一个整体。这里的元组包含三个元素：
% reply：是一个原子，一般作为消息的类型标识，用于接收方识别消息的用途。
% Ref：消息引用，接收方可以借助这个引用把回复和对应的请求关联起来。
% Reply：具体的回复内容。



%% error_node_down 函数用于生成 节点宕机 的错误信息
%% NodeId 是节点的标识，调用 error_node 函数生成错误信息
error_node_down(NodeId) ->
  error_node(NodeId, ?EH_NODEDOWN).
% 第一个参数：NodeId 即 error_node_down 函数接收的参数，代表出现宕机问题的节点标识。
% 第二个参数：?EH_NODEDOWN 是一个宏。在 Erlang 中，以 ? 开头的标识符通常是宏。
% 宏是在编译时被替换的代码片段，?EH_NODEDOWN 可能在 erlang_craq.hrl 头文件中定义，用于表示节点宕机这一特定状态或错误码。


%% error_node_unavailable 函数用于生成 节点不可用 的错误信息
%% NodeId 是节点的标识，调用 error_node 函数生成错误信息
error_node_unavailable(NodeId) ->
  error_node(NodeId, ?EH_NODE_UNAVAILABLE).

%% error_being_updated 函数用于生成 数据正在被更新 的错误信息
%% DataList 是数据列表，调用 object 函数生成错误信息
error_being_updated(DataList) -> 
  object(error, DataList, ?EH_BEING_UPDATED).

%% updated 函数用于生成 数据已更新 的成功信息
%% DataList 是数据列表，调用 object 函数生成成功信息
updated(DataList) ->
  object(ok, DataList, ?EH_UPDATED).
 
%% object_tuple 函数用于生成 含对象列表和消息的元组
%% DataList 是数据列表，Msg 是消息内容
object_tuple(DataList, Msg) ->
  {eh_update_msg:get_object_list(DataList), Msg}.

%% node_tuple 函数用于生成 包含节点 ID 和消息的元组
%% NodeId 是节点的标识，Msg 是消息内容
node_tuple(NodeId, Msg) ->
  {NodeId, Msg}.

%% error_node 函数用于生成 包含错误信息的元组
%% NodeId 是节点的标识，Msg 是消息内容
error_node(NodeId, Msg) ->
  {error, node_tuple(NodeId, Msg)}.

%% object 函数用于生成包含标签、对象列表和消息的元组
%% Tag 是标签，DataList 是数据列表，Msg 是消息内容
object(Tag, DataList, Msg) ->
  {Tag, object_tuple(DataList, Msg)}.

%% process_pending 函数用于处理挂起的查询
%% ObjectType 是对象类型，ObjectId 是对象标识，State 是系统状态
%% 从系统状态中提取 query_data 和 app_config
process_pending(ObjectType, ObjectId, #eh_system_state{query_data=QueryData, app_config=AppConfig}=State) ->
  %% 使用 eh_system_util:find_map 函数在 QueryData 中查找 {ObjectType, ObjectId}
  case eh_system_util:find_map({ObjectType, ObjectId}, QueryData) of
    error      ->
      %% 当 find_map 函数返回 error 时，表示未找到匹配项，此时该分支被执行，返回 ok。
      ok;
    {ok, []}   ->
      %% 若找到空列表，返回 ok
      ok;
    {ok, List} ->
      %% 若找到非空列表，调用 query_reply 函数获取查询回复
      QueryReply = query_reply(ObjectType, ObjectId, AppConfig),
      %% 遍历列表中的每个元素 {From, Ref}，调用 reply 函数发送回复消息
      lists:foreach(fun({From, Ref}) -> reply(From, Ref, {ok, QueryReply}) end, List)
  end,
  %% 调用 eh_node_timestamp:update_state_remove_query_data 函数更新系统状态，移除查询数据
  eh_node_timestamp:update_state_remove_query_data(ObjectType, ObjectId, State).


% #eh_system_state{query_data=QueryData, app_config=AppConfig}=State：这是一个记录模式匹配。
% #eh_system_state{} 是 Erlang 中的记录语法，用于表示 eh_system_state 类型的记录。
% 通过 query_data=QueryData 和 app_config=AppConfig 从 State 记录中提取 query_data 和 app_config 字段的值，
% 并分别绑定到 QueryData 和 AppConfig 变量上。



%% 另一个版本的 process_pending 函数，处理包含多个更新消息的列表
%% UMsgList 是更新消息列表，State 是系统状态
process_pending(UMsgList, State) ->
  %% 使用 lists:foldl 函数遍历 UMsgList，对每个元素调用 process_pending 函数处理
  lists:foldl(fun({ObjectType, ObjectId, _}, StateAcc) -> process_pending(ObjectType, ObjectId, StateAcc) end, State, UMsgList).

%% query_reply 函数用于获取查询结果
%% ObjectType 是对象类型，ObjectId 是对象标识，AppConfig 是应用配置
query_reply(ObjectType, ObjectId, AppConfig) ->
  %% 从应用配置中获取复制数据管理器
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  %% 调用复制数据管理器的 query 方法进行查询
  ReplDataManager:query({ObjectType, ObjectId}).

%% query 函数用于处理查询请求
%% Tag 是标签，ObjectType 是对象类型，ObjectId 是对象标识，From 是发送者进程 ID，Ref 是消息引用，State 是系统状态
query(Tag, ObjectType, ObjectId, From, Ref, #eh_system_state{app_config=AppConfig}=State) ->
  %% 调用 reply 函数发送包含标签和查询结果的回复消息
  reply(From, Ref, {Tag, query_reply(ObjectType, ObjectId, AppConfig)}),
  %% 返回系统状态
  State.

%% process_tail 函数用于将查询请求转发到尾节点
%% ObjectType 是对象类型，ObjectId 是对象标识，Tail 是尾节点 ID，From 是发送者进程 ID，Ref 是消息引用，State 是系统状态
process_tail(ObjectType, ObjectId, Tail, From, Ref, State) ->
  %% 使用 gen_server:cast 函数异步发送查询请求到尾节点的系统服务器
  gen_server:cast({?EH_SYSTEM_SERVER, Tail}, {?EH_QUERY_AQ, {ObjectType, ObjectId, From, Ref}}),
  %% 返回系统状态
  State.


% gen_server:cast 函数：
%   gen_server 是 Erlang 标准库中的行为模块，cast 是该模块提供的函数，用于向 gen_server 进程异步发送消息。
%   函数原型为 gen_server:cast(ServerRef, Request) -> ok。
% ServerRef 参数：
%   {?EH_SYSTEM_SERVER, Tail} 是目标 gen_server 进程的引用。
%   ?EH_SYSTEM_SERVER 是以 ? 开头的宏，可能在 erlang_craq.hrl 头文件中定义，通常代表服务器进程的注册名；
%   Tail 是尾节点的标识，这个元组表示在 Tail 节点上注册名为 ?EH_SYSTEM_SERVER 的进程。
% Request 参数：
%   {?EH_QUERY_AQ, {ObjectType, ObjectId, From, Ref}} 是要发送的消息内容。
%   ?EH_QUERY_AQ 同样是宏，可能表示查询请求的类型；
%   内层元组 {ObjectType, ObjectId, From, Ref} 包含了查询对象的类型、标识、发送请求的进程 ID 以及消息引用。
% 返回值：
%   gen_server:cast 函数会立即返回 ok，表示消息已成功发送，但不保证目标进程已经处理该消息。
% 
