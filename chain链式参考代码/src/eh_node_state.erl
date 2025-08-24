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

% 管理和更新节点的状态
-module(eh_node_state).

-export([update_state_msg/1,
         update_state_snapshot/1,
         update_state_ready/1,
         update_state_transient/1,
         data_state/1,
         client_state/1,
         snapshot_state/1,
         msg_state/1,
         display_state/1]).

-include("erlang_craq.hrl").

% 更新 eh_system_state 记录中的 node_status 字段为传入的 NodeStatus。
update_state(NodeStatus, State) ->
  State#eh_system_state{node_status=NodeStatus}.
% Erlang 中记录更新的语法，#eh_system_state 表示操作的记录类型，node_status=NodeStatus 表示将 node_status 字段更新为 NodeStatus。


% 消息相关的状态变换

update_state_msg(#eh_system_state{node_status=?EH_TRANSIENT}=State) ->
  update_state(?EH_TRANSIENT_TU, State);    %临时->时间戳更新完成
update_state_msg(#eh_system_state{node_status=?EH_TRANSIENT_DU}=State) ->
  update_state(?EH_READY, State);    %数据更新完成->就绪
update_state_msg(State) ->
  State.  %不变
% 模式匹配，通过 #eh_system_state{node_status=...} 匹配不同状态的 eh_system_state 记录，不同模式对应不同的处理逻辑。


% 快照相关的状态变换

update_state_snapshot(#eh_system_state{node_status=?EH_TRANSIENT}=State) ->
  update_state(?EH_TRANSIENT_DU, State);  % （临时-> 数据更新完成）
update_state_snapshot(#eh_system_state{node_status=?EH_TRANSIENT_TU}=State) ->
  update_state(?EH_READY, State);  % （临时-时间戳更新完成->就绪）  
update_state_snapshot(State) ->
  State.

update_state_ready(State) ->
  update_state(?EH_READY, State).

update_state_transient(State) ->
  update_state(?EH_TRANSIENT, State).

% 依据节点的状态和待处理预消息数据，决定返回节点就绪状态还是未就绪状态。

data_state(#eh_system_state{node_status=?EH_READY}) ->  %如果节点就绪，则返回就绪
  ?EH_READY;
data_state(#eh_system_state{node_status=?EH_TRANSIENT_DU, pending_pre_msg_data=PendingPreMsgData}) -> %如果 临时-数据更新完成状态，检查【待处理预消息数据】是否为空
  case eh_system_util:is_empty_map(PendingPreMsgData) of 
    true  -> % 空，认为就绪
      ?EH_READY;
    false -> %存在待处理预消息，未就绪
      ?EH_NOT_READY
  end;
data_state(_) ->  %如果是其他状态，认为未就绪
  ?EH_NOT_READY.


%% @doc 根据节点状态判断快照状态。
%%      若节点处于就绪状态或临时 - 数据更新完成状态，认为快照状态也就绪；
%%      若节点处于其他状态，认为快照状态未就绪。
%% @spec snapshot_state(State :: #eh_system_state{}) -> ?EH_READY | ?EH_NOT_READY.
%% @param State 节点的当前状态记录，包含节点的各种状态信息。
%% @return 若快照状态就绪，返回 ?EH_READY；若未就绪，返回 ?EH_NOT_READY。
%% @end
snapshot_state(#eh_system_state{node_status=?EH_READY}) ->  % 如果节点状态为就绪状态，说明节点可正常工作，快照状态也就绪
  ?EH_READY;
snapshot_state(#eh_system_state{node_status=?EH_TRANSIENT_DU}) -> % 如果节点状态为临时 - 数据更新完成状态，意味着数据已更新完毕，快照状态就绪
  ?EH_READY;
snapshot_state(_) ->  % 如果节点处于除就绪和临时 - 数据更新完成之外的其他状态，认为快照状态未就绪
  ?EH_NOT_READY.



%% @doc 根据节点状态判断客户端状态。
%%      若节点处于就绪状态，认为客户端状态也就绪；
%%      若节点处于其他状态，认为客户端状态未就绪。
%% @spec client_state(State :: #eh_system_state{}) -> ?EH_READY | ?EH_NOT_READY.
%% @param State 节点的当前状态记录，包含节点的各种状态信息。
%% @return 若客户端状态就绪，返回 ?EH_READY；若未就绪，返回 ?EH_NOT_READY。
%% @end
client_state(#eh_system_state{node_status=?EH_READY}) -> % 当节点状态为就绪状态时，返回客户端状态就绪
  ?EH_READY;
client_state(_) ->
  ?EH_NOT_READY.


%% @doc 根据节点状态判断消息处理状态。
%%      若节点处于未就绪状态（?EH_NOT_READY），认为消息处理状态未就绪；
%%      若节点处于其他状态，认为消息处理状态就绪。
%% @spec msg_state(State :: #eh_system_state{}) -> ?EH_READY | ?EH_NOT_READY.
%% @param State 节点的当前状态记录，包含节点的各种状态信息。
%% @return 若消息处理状态就绪，返回 ?EH_READY；若未就绪，返回 ?EH_NOT_READY。
%% @end
msg_state(#eh_system_state{node_status=?EH_NOT_READY}) ->
  % 当节点状态为未就绪状态时，返回消息处理状态未就绪
  ?EH_NOT_READY;
msg_state(_) ->
  % 当节点处于除未就绪状态之外的其他状态时，返回消息处理状态就绪
  ?EH_READY.


%% @doc 显示节点的当前状态。
%%      该函数接收一个 `#eh_system_state{}` 记录，从中提取节点状态信息，
%%      并调用 `eh_system_util` 模块的 `display_atom_to_list/1` 函数将节点状态原子转换为字符串进行显示。
%% @spec display_state(State :: #eh_system_state{}) -> string().
%% @param State 节点的当前状态记录，包含节点的各种状态信息。
%% @return 表示节点状态的字符串。
%% @end
display_state(#eh_system_state{node_status=NodeStatus}) ->
  % 从传入的节点状态记录中提取节点状态，调用 eh_system_util 模块的函数将其转换为字符串
  eh_system_util:display_atom_to_list(NodeStatus).




