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

-module(eh_event_handler).

-behavior(gen_event).

-export([add_handler/1, delete_handler/0]).

-export([init/1, handle_call/2, handle_info/2, handle_event/2, terminate/2, code_change/3]).

-export([get_msg_data/1]).

-include("erlang_craq.hrl").

% 注册事件处理程序。
% 传入当前模块名和配置
add_handler(AppConfig) ->
  eh_event:add_handler(?MODULE, AppConfig).

% 删除当前模块的事件处理程序。
delete_handler() ->
  eh_event:delete_handler(?MODULE, []).

% 初始化事件处理器状态。
% 如果配置为 standard_io，则输出到控制台；否则打开指定日志文件。
init(AppConfig) ->
  File1 = case eh_system_config:get_file_repl_log(AppConfig) of
            standard_io ->
              standard_io;
            FileName ->
              {ok, File} = open(FileName),
              File
          end, 
  {ok, File1}.

% 终止事件处理器时关闭文件句柄。
terminate(_Args, standard_io) ->
  ok;
terminate(_Args, File) ->
  close(File).

% 用于代码热更新，忽略版本号和额外参数，保持状态不变。
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% 处理同步请求，不做任何操作。
handle_call(_Request, State) ->
  {ok, ok, State}.

% 处理非事件信息（如系统消息）。
% 忽略消息内容，保持状态不变
handle_info(_Info, State) ->
  {ok, State}.

% 处理事件。写入日志
handle_event({Fmt, Args}, File) ->
    io:fwrite(File, Fmt, Args),
    {ok, File}.

% 日志格式化
get_msg_data({data_state, {Module, Msg, #eh_data_state{timestamp=Timestamp, data_index_list=DIList}}}) ->
  {"[~p] ~p timestamp=~p, data_index_list=~p~n", [Module, Msg, Timestamp, DIList]};

get_msg_data({state, {Module, Msg, StateData}}) ->
  NodeId = eh_system_util:get_node_name(StateData#eh_system_state.app_config#eh_app_config.node_id),
  Pred = eh_system_util:get_node_name(StateData#eh_system_state.predecessor),
  Succ = eh_system_util:get_node_name(StateData#eh_system_state.successor),
  Timestamp = StateData#eh_system_state.timestamp,
  NodeState = eh_node_state:display_state(StateData),
  ReplRing = eh_system_util:make_list_to_string(fun eh_system_util:get_node_name/1, StateData#eh_system_state.repl_ring),
  PreMsgData = list_msg_map(StateData#eh_system_state.pre_msg_data),
  MsgData = list_msg_map(StateData#eh_system_state.msg_data),
  CSet = list_msg_set_value(StateData#eh_system_state.completed_set),
  {"[~p] ~p node_status=~p, node_id=~p, repl_ring=~p, pred=~p, succ=~p, timestamp=~p, pre_msg_data=~p, msg_data=~p, completed_set=~p~n",
   [Module, Msg, NodeState, NodeId, ReplRing, Pred, Succ, Timestamp, PreMsgData, MsgData, CSet]};

get_msg_data({message, {Module, Msg, {UMsgList, CompletedSet}}}) ->
  RCSet = list_msg_set_value(CompletedSet),
  UMList = list_msg_list(UMsgList),
  {"[~p] ~p message=~p, completed_set=~p~n", [Module, Msg, UMList, RCSet]};

get_msg_data({data, {Module, Msg, {DataMsg, Data}}}) ->
  {"[~p] ~p ~p=~p~n", [Module, Msg, DataMsg, Data]}.

% 判断值是否为可格式化类型
is_listable_value(Value) ->
  is_atom(Value) orelse is_integer(Value) orelse is_float(Value) orelse is_list(Value).

% 判断消息键是否为可格式化类型
is_listable_msg_key(#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId}) ->
  is_listable_value(Timestamp) andalso is_listable_value(ObjectType) andalso is_listable_value(ObjectId).

% 各种类型转换为字符串
list_value(Value) when is_list(Value) ->
  Value;
list_value(Value) when is_atom(Value) ->
  atom_to_list(Value);
list_value(Value) when is_integer(Value) ->
  integer_to_list(Value);
list_value(Value) when is_float(Value) ->
  float_to_list(Value).

% 将消息键转换为字符串。
list_msg_key(#eh_update_msg_key{timestamp=Timestamp, object_type=ObjectType, object_id=ObjectId}=UMsgKey) ->
  case is_listable_msg_key(UMsgKey) of
     true  ->
       "{"++list_value(Timestamp)++","++list_value(ObjectType)++","++list_value(ObjectId)++"}";
     false ->
       list_value(Timestamp)
  end.

% 格式化节点消息。
list_node_msg(NodeId, MsgValue) ->
  eh_system_util:get_node_name(NodeId)++"=>"++MsgValue.

% 将消息键和节点 ID 转换为字符串。
list_msg(UMsgKey, MsgNodeId) ->
  list_node_msg(MsgNodeId, list_msg_key(UMsgKey)).

% 向列表字符串中添加新元素。
add_list(List, Acc) when length(Acc) =:= 0 ->
  Acc++List;
add_list(List, Acc) ->
  Acc++","++List.

% 遍历集合中的每个消息键，转换为字符串。
list_msg_set_value(Set) ->
  eh_system_util:fold_set(fun(MsgKey, Acc) -> add_list(list_msg_key(MsgKey), Acc) end, [], Set).

% 历映射中的每个键值对，转换为字符串。
list_msg_map(Map) ->
  eh_system_util:fold_map(fun(MsgKey, #eh_update_msg_data{node_id=NodeId}, Acc) -> add_list(list_msg(MsgKey, NodeId), Acc) end, [], Map).

list_msg_list(List) ->
  lists:foldl(fun({MsgKey, #eh_update_msg_data{node_id=NodeId}}, Acc) -> add_list(list_msg(MsgKey, NodeId), Acc) end, [], List).

% 打开文件用于写入。
open(FileName) ->
  file:open(FileName, [write]).%file:open/2：标准库函数，打开文件。

% 关闭文件句柄。
close(File) ->
  file:close(File).







  

