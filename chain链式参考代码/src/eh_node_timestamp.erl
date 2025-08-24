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

-module(eh_node_timestamp).

-export([update_state_client_reply/2,
         update_state_completed_set/2,
         update_state_new_msg/3,
         update_state_timestamp/2,
         update_state_msg_data/2,
         update_state_add_query_data/5,
         update_state_remove_query_data/3,
         valid_pre_update_msg/2,
         valid_update_msg/2,
         valid_pending_pre_msg_data/2,
         valid_add_node_msg/2,
         msg_status/2]).

-include("erlang_craq.hrl").

%% @doc 向查询数据中添加新的查询请求信息。
%% 若指定的对象类型和 ID 对应的查询数据已存在，则将新的查询请求添加到列表头部；
%% 若不存在，则创建一个新的列表。
%% @spec update_state_add_query_data(ObjectType :: term(), ObjectId :: term(), From :: term(), Ref :: term(), State :: #eh_system_state{}) -> #eh_system_state{}
%% @param ObjectType 对象类型，用于标识查询对象的类型。
%% @param ObjectId 对象 ID，用于唯一标识查询对象。
%% @param From 请求发起者，记录查询请求的来源。
%% @param Ref 请求引用，用于唯一标识查询请求。
%% @param #eh_system_state{query_data=QueryData} 当前系统状态，包含查询数据。
%% @returns 返回更新后的系统状态，其中查询数据已更新。
update_state_add_query_data(ObjectType, ObjectId, From, Ref, #eh_system_state{query_data=QueryData}=State) ->
  % 生成查询数据的键，由对象类型和对象 ID 组成
  Key = {ObjectType, ObjectId},
  % 生成查询数据的值，由请求发起者和请求引用组成
  Value = {From, Ref},
  % 检查查询数据中是否已存在该键
  List1 = case eh_system_util:find_map(Key, QueryData) of
            % 若不存在，创建一个新的列表
            error      ->
              [Value];
            % 若存在，将新的值添加到列表头部
            {ok, List} ->
              [Value | List]
          end,
  % 更新系统状态中的查询数据
  State#eh_system_state{query_data=eh_system_util:add_map(Key, List1, QueryData)}.

%% @doc 从查询数据中移除指定对象类型和 ID 对应的查询请求信息。
%% @spec update_state_remove_query_data(ObjectType :: term(), ObjectId :: term(), State :: #eh_system_state{}) -> #eh_system_state{}
%% @param ObjectType 对象类型，用于标识要移除的查询对象的类型。
%% @param ObjectId 对象 ID，用于唯一标识要移除的查询对象。
%% @param #eh_system_state{query_data=QueryData} 当前系统状态，包含查询数据。
%% @returns 返回更新后的系统状态，其中指定的查询数据已移除。
update_state_remove_query_data(ObjectType, ObjectId, #eh_system_state{query_data=QueryData}=State) ->
  % 从查询数据中移除指定键对应的条目，并更新系统状态
  State#eh_system_state{query_data=eh_system_util:remove_map({ObjectType, ObjectId}, QueryData)}.

%% @doc 处理客户端回复消息，更新预消息数据、消息数据和已完成集合。
%% @spec update_state_client_reply(UMsgList :: list(), State :: #eh_system_state{}) -> #eh_system_state{}
%% @param UMsgList 更新消息列表，包含需要处理的客户端回复消息。
%% @param #eh_system_state{successor=Succ, completed_set=CompletedSet, pre_msg_data=PreMsgData, msg_data=MsgData} 当前系统状态，
%%        包含后继节点、已完成集合、预消息数据和消息数据。
%% @returns 返回更新后的系统状态，其中预消息数据、消息数据和已完成集合已更新。
update_state_client_reply(UMsgList, 
                          #eh_system_state{successor=Succ, completed_set=CompletedSet, pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  % 遍历更新消息列表，从预消息数据中移除消息，并将消息键添加到已完成集合
  {PreMsgData1, CompletedSet1} = lists:foldl(fun({UMsgKeyX, _}, {MsgDataX, CompletedSetX}) -> 
                                             {eh_system_util:remove_map(UMsgKeyX, MsgDataX), eh_system_util:add_set(UMsgKeyX, CompletedSetX)} end, %匿名函数 从 MsgDataX 中移除 UMsgKeyX 对应的条目。把 UMsgKeyX 添加到 CompletedSetX 中。
                                             {PreMsgData, CompletedSet}, %初始累加器  lists:foldl/3 的第二个参数，作为折叠操作的初始状态。PreMsgData 是初始的预消息数据，CompletedSet 是初始的已完成集合。
                                             UMsgList),%要处理的列表  lists:foldl/3 的第三个参数，是一个包含多个 {UMsgKey, UMsgData} 元组的列表。
  % 遍历更新消息列表，从消息数据中移除消息，并将消息键添加到已完成集合
  {MsgData1, CompletedSet2} = lists:foldl(fun({UMsgKeyX, _}, {MsgDataX, CompletedSetX}) -> 
                                             {eh_system_util:remove_map(UMsgKeyX, MsgDataX), eh_system_util:add_set(UMsgKeyX, CompletedSetX)} end,
                                             {MsgData, CompletedSet1},
                                             UMsgList),
  % why？？？？不解？？？？？？？？为什么尾结点不变
  
  % 根据后继节点是否存在，决定最终的已完成集合
  CompletedSet3 = case Succ of
                    % 若后继节点不存在，使用原始的已完成集合
                    undefined ->
                       CompletedSet; 
                    _         ->
                       CompletedSet2
                  end,
  % 更新系统状态中的已完成集合、预消息数据和消息数据
  State#eh_system_state{completed_set=CompletedSet3, pre_msg_data=PreMsgData1, msg_data=MsgData1}.




%% @doc 更新系统状态中的已完成集合，计算两个集合的差集。
% 维护一个去重的已完成任务列表，确保系统不会重复处理相同的任务。
% 当接收到新的已完成任务列表时，系统会移除那些已经被确认完成的任务，保留尚未完成的任务记录。
update_state_completed_set(CompletedSet,  %新的已完成集合
                           #eh_system_state{completed_set=NewCompletedSet}=State) -> %原有的已完成集合
  % 计算原已完成集合与新已完成集合的差集，并更新系统状态
  State#eh_system_state{completed_set=eh_system_util:subtract_set(NewCompletedSet, CompletedSet)}.

%% @doc 根据已完成集合更新系统状态中的消息数据，移除已完成的消息。
%% @param CompletedSet 已完成集合，包含已完成的消息键。
%% @param #eh_system_state{msg_data=MsgData} 当前系统状态，包含消息数据。
%% @returns 返回更新后的系统状态，其中消息数据已移除已完成的消息。
update_state_msg_data(CompletedSet,
                      #eh_system_state{msg_data=MsgData}=State) ->
  % 遍历已完成集合，从消息数据中移除对应的消息
  MsgData1 = eh_system_util:fold_set(fun(X, Acc) -> eh_system_util:remove_map(X, Acc) end, MsgData, CompletedSet),
  % 更新系统状态中的消息数据
  State#eh_system_state{msg_data=MsgData1}.


%% @doc 更新系统状态中的预消息数据和消息数据。
%% 当消息标签为 ?EH_PRED_PRE_UPDATE 时，将更新消息列表中的消息添加到预消息数据中。
%% 当消息标签为 ?EH_SUCC_UPDATE 时，从预消息数据中移除更新消息列表中的消息，
%% 并将这些消息添加到消息数据中。

%% 若是（预更新阶段），则将更新消息列表UMsgList中的消息添加到预消息数据PreMsgData中
update_state_new_msg(?EH_PRED_PRE_UPDATE, 
                     UMsgList,  %更新消息列表，包含多个 {UMsgKey, UMsgData} 元组，待处理的消息。
                     #eh_system_state{pre_msg_data=PreMsgData}=State) ->
  PreMsgData1 = lists:foldl(fun({UMsgKey, UMsgData}, PreMsgDataX) -> 
                            % 调用 eh_system_util:add_map 函数，将 UMsgKey 和 UMsgData 添加到 PreMsgDataX 中
                            eh_system_util:add_map(UMsgKey, UMsgData, PreMsgDataX) 
                        end, 
                        PreMsgData, 
                        UMsgList),
  State#eh_system_state{pre_msg_data=PreMsgData1};

%% 若是（成功更新阶段），从预消息数据PreMsgData中移除每个消息，并将其添加到消息数据MsgData中
update_state_new_msg(?EH_SUCC_UPDATE, 
                     UMsgList, 
                     #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  {PreMsgData1, MsgData1} = lists:foldl(fun({UMsgKey, UMsgData}, {PreMsgDataX, MsgDataX}) -> 
                                        % 从预消息数据中移除 UMsgKey 对应的消息
                                        {eh_system_util:remove_map(UMsgKey, PreMsgDataX), 
                                        % 将 UMsgKey 和 UMsgData 添加到消息数据中
                                        eh_system_util:add_map(UMsgKey, UMsgData, MsgDataX)} 
                                    end,
                                    {PreMsgData, MsgData}, 
                                    UMsgList),
  State#eh_system_state{pre_msg_data=PreMsgData1, msg_data=MsgData1}.

% 更新系统状态中的时间戳，比较传入的消息时间戳和系统当前时间戳，取两者中的最大值作为新的系统时间戳。
update_state_timestamp(MsgTimestamp, 
                       #eh_system_state{timestamp=Timestamp}=State) ->
  eh_node_state:update_state_msg(State#eh_system_state{timestamp=max(MsgTimestamp, Timestamp)}).


%% @doc 根据节点 ID 和系统状态，判断添加节点消息的有效性。
valid_add_node_msg(Node, 
                   #eh_system_state{repl_ring=ReplRing, app_config=AppConfig}=State) ->
  case {eh_node_state:msg_state(State), Node =:= eh_system_config:get_node_id(AppConfig), lists:member(Node, ReplRing)} of % 节点状态、是否为当前节点、是否已存在于环中
    {?EH_NOT_READY, true, false} -> 
      ?EH_VALID_FOR_NEW; % 该添加命令是有效的，且是针对新系统初始化时添加第一个节点
    {?EH_READY, false, false}    -> 
      ?EH_VALID_FOR_EXISTING; % 该添加命令是有效的，且是针对现有系统中添加新节点的操作
    {_, _, _}                    -> 
      ?EH_INVALID_MSG % 该消息无效
  end.



%% @doc 验证待处理的预消息数据的有效性。
%% 该函数检查PendingPreMsgData中的所有键是否既不在PreMsgData中也不在MsgData中存在，
%% 确保待处理的预消息数据不会与现有的预消息数据和消息数据产生冲突。
%% @returns 如果所有消息键都既不存在于预消息数据中，也不存在于消息数据中，则返回 true；否则返回 false。
valid_pending_pre_msg_data(PendingPreMsgData, #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}) ->
  Flag = eh_system_util:fold_map(fun(UMsgKey, _, FlagX) -> 
                                   FlagX andalso (not eh_system_util:is_key_map(UMsgKey, PreMsgData)) %逻辑判断：FlagX andalso (not eh_system_util:is_key_map(UMsgKey, PreMsgData)) 借助 andalso 逻辑运算符，只有当 FlagX 为 true 时，才会检查 UMsgKey 是否存在于 PreMsgData 中。若存在，结果为 false。
                               end, true, PendingPreMsgData),
  % 使用上一步得到的标志作为初始值，只要有一个消息键存在于消息数据中，最终结果就会为 false
  eh_system_util:fold_map(fun(UMsgKey, _, FlagX) -> 
                           FlagX andalso (not eh_system_util:is_key_map(UMsgKey, MsgData)) 
                       end, Flag, PendingPreMsgData). 




%% @doc 根据更新消息列表和系统状态，判断消息的状态（头部消息、尾部消息或环消息）。
%% @param UMsgList 更新消息列表，包含待处理的消息信息。
%% @returns 返回消息的状态，可能的值为 ?EH_HEAD_MSG、?EH_TAIL_MSG 或 ?EH_RING_MSG。
msg_status(UMsgList, 
          #eh_system_state{repl_ring_order=ReplRingOrder, repl_ring=ReplRing, app_config=AppConfig}) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  % 从更新消息列表中获取消息的节点 ID
  {_, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
  % 根据消息节点 ID、复制环、复制环顺序和当前节点顺序，计算有效的尾部节点 ID
  TailNodeId = eh_repl_ring:effective_tail_node_id(MsgNodeId, ReplRing, ReplRingOrder, NodeOrder),
  % 根据消息节点 ID、复制环、复制环顺序和当前节点顺序，计算有效的头部节点 ID
  HeadNodeId = eh_repl_ring:effective_head_node_id(MsgNodeId, ReplRing, ReplRingOrder, NodeOrder),
  % 根据当前节点 ID 与头部节点 ID、尾部节点 ID 的比较结果，判断消息状态
  case {NodeId =:= HeadNodeId, NodeId =:= TailNodeId} of
    % 若当前节点 ID 等于头部节点 ID 且不等于尾部节点 ID，则为头部消息
    {true, false}  ->
      ?EH_HEAD_MSG;
    % 若当前节点 ID 不等于头部节点 ID 且等于尾部节点 ID，则为尾部消息
    {false, true}  ->
      ?EH_TAIL_MSG;
    % 若当前节点 ID 既不等于头部节点 ID 也不等于尾部节点 ID，则为环消息
    {false, false} ->
      ?EH_RING_MSG
  end.


%% @doc 处理待处理的预消息数据
%% 
%% 该函数检查系统状态中是否存在待处理的预消息数据，如果存在则根据消息列表中的时间戳和节点ID
%% 来筛选和处理相应的预消息数据，并将处理后的数据持久化。
%%
%% @param UMsgList 消息列表，用于获取消息参数（时间戳、节点ID等）
%% @param State 系统状态记录，包含pending_pre_msg_data字段用于存储待处理的预消息数据
%% @return 返回更新后的系统状态，其中待处理的预消息数据已被清空

process_pending_pre_msg_data(UMsgList, #eh_system_state{pending_pre_msg_data=PendingPreMsgData}=State) ->
  %% 检查是否存在待处理的预消息数据
  case eh_system_util:size_map(PendingPreMsgData) > 0 of
    true  ->
      %% 从消息列表中提取消息参数，包括时间戳和节点ID
      {MsgTimestamp, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
      
      %% 根据时间戳和节点ID对预消息数据进行分区，筛选出需要处理的消息
      {_, MsgMap} = eh_update_msg:partition_on_timestamp_node_id(MsgTimestamp, MsgNodeId, PendingPreMsgData),
      
      %% 如果存在需要处理的消息，则进行数据持久化操作
      State1 = case eh_system_util:size_map(MsgMap) > 0 of
                 true  ->
                   eh_persist_data:persist_data(eh_system_util:to_list_map(MsgMap), State);
                 false -> %不存在则保持原状态
                   State
               end,
      
      %% 清空待处理的预消息数据并返回更新后的状态
      % 不管是否进行了持久化操作，
      State1#eh_system_state{pending_pre_msg_data=eh_system_util:new_map()};
    false ->
      %% 不存在待处理数据时，直接返回原状态
      State
  end.



%% @doc 验证消息是否有效并处理冲突解决
%% 
%% 该函数通过多个检查步骤来验证消息列表的有效性，并在必要时调用冲突解决函数。
%% 验证过程包括预处理待处理消息、检查映射关系、执行自定义检查函数以及检查数据一致性。
%% 如果所有检查都失败，则调用冲突解决函数来尝试解决冲突。
%%
%% @param CheckFun 自定义检查函数，用于对消息列表和检查数据进行验证
%%                 函数签名: fun((UMsgList, CheckData) -> boolean())
%% @param ConflictResolveFun 冲突解决函数，当所有检查都失败时调用
%%                          函数签名: fun((UMsgList, State) -> {boolean(), NewState})
%% @param UMsgList 消息列表，包含待验证的消息
%% @param MsgData 消息数据，用于映射关系检查
%% @param CheckData 检查数据，传递给自定义检查函数
%% @param State 状态数据，包含当前的系统状态
%%
%% @return {boolean(), MsgStatus | undefined, NewState}
%%         返回三元组：{是否需要解决冲突, 消息状态, 新状态}
%%         - 当需要解决冲突且解决成功时，返回 {true, MsgStatus, State}
%%         - 当需要解决冲突但解决失败时，返回 {false, undefined, State}
%%         - 当不需要解决冲突时，返回 {false, undefined, State}

valid_msg(CheckFun,
          ConflictResolveFun, 
          UMsgList,
          MsgData,
          CheckData,
          State) ->
  %% 预处理待处理的消息数据
  State1 = process_pending_pre_msg_data(UMsgList, State),
  %% 检查消息列表与消息数据的映射关系
  Flag1 = check_map(UMsgList, MsgData),
  %% 如果映射检查失败，则执行自定义检查函数
  Flag2 = Flag1 orelse CheckFun(UMsgList, CheckData),
  %% 如果前面的检查都失败，则检查数据一致性
  Flag3 = Flag2 orelse check_data(UMsgList, State1),
  case Flag3 of
    false ->
      %% 所有检查都失败，调用冲突解决函数
      {Flag4, State2} = ConflictResolveFun(UMsgList, State1),
      case Flag4 of
        true  ->
          %% 冲突解决成功，返回消息状态和新状态
          {true, msg_status(UMsgList, State2), State2};
        false ->
          %% 冲突解决失败，返回未定义消息状态和新状态
          {false, undefined, State2}
      end;
    true  ->
      %% 检查通过，不需要解决冲突
      {false, undefined, State1}
  end.




%% @doc 验证更新消息列表的有效性
%% 
%% 该函数用于检查一组更新消息是否可以被安全地应用到当前系统状态中。
%% 它通过调用valid_msg/6函数，使用check_set/2进行集合检查，
%% 并使用update_conflict_resolver/2来解决可能的冲突。
%%
%% @param UMsgList 更新消息列表，包含需要验证的消息
%% @param State 系统状态记录，包含消息数据和已完成集合
%% @param State.msg_data 消息数据存储
%% @param State.completed_set 已完成的消息集合
%% @return 返回验证结果，具体格式由valid_msg/6函数决定
valid_update_msg(UMsgList,
                 #eh_system_state{msg_data=MsgData, completed_set=CompletedSet}=State) ->
  valid_msg(fun check_set/2,
            fun update_conflict_resolver/2,
            UMsgList,
            MsgData,
            CompletedSet, 
            State).


valid_pre_update_msg(UMsgList,
                     #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  valid_msg(fun check_map/2,
            fun pre_update_conflict_resolver/2, 
            UMsgList, 
            PreMsgData, 
            MsgData, 
            State).

%% @doc 检查更新消息列表中的数据是否有效。
%% 该函数会比较系统时间戳和消息时间戳，仅当系统时间戳大于等于消息时间戳时，
%% 才会调用复制数据管理器进一步检查消息数据。
%% @spec check_data(UMsgList :: list(), State :: #eh_system_state{}) -> boolean()
%% @param UMsgList 更新消息列表，包含待检查的消息数据。
%% @param #eh_system_state{timestamp=Timestamp, app_config=AppConfig} 当前系统状态，
%%        包含系统时间戳和应用配置信息。
%% @returns 若系统时间戳大于等于消息时间戳且数据检查通过，返回 `true`；否则返回 `false`。
check_data(UMsgList,
           #eh_system_state{timestamp=Timestamp, app_config=AppConfig}) ->
  % 从更新消息列表中提取消息时间戳和数据列表
  {MsgTimestamp, _, _, _, DataList} = eh_update_msg:get_data_list(UMsgList),
  % 比较系统时间戳和消息时间戳
  case Timestamp >= MsgTimestamp of
    % 若系统时间戳大于等于消息时间戳
    true  -> 
      % 从应用配置中获取复制数据管理器模块
      ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
      % 调用复制数据管理器的 check_data 函数检查消息数据
      ReplDataManager:check_data({MsgTimestamp, DataList});
    % 若系统时间戳小于消息时间戳
    false ->
      % 直接返回 false，表示数据无效
      false
  end.


%% @doc 检查消息列表中的消息是否存在于指定的消息映射中。
%% 通过消息的时间戳和节点 ID 对消息映射进行分区，判断是否存在匹配的消息。
%% @spec check_map(UMsgList :: list(), MsgMap :: map()) -> boolean()
%% @param UMsgList 更新消息列表，包含待检查的消息信息。
%% @param MsgMap 消息映射，存储消息的键值对。
%% @returns 若存在匹配的消息，返回 `true`；否则返回 `false`。
check_map(UMsgList, MsgMap) ->
  % 从更新消息列表中提取消息的时间戳和节点 ID
  {Timestamp, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
  % 根据提取的时间戳和节点 ID 对消息映射进行分区，
  % TMap 包含匹配时间戳和节点 ID 的消息，第二个元素被忽略
  {TMap, _} = eh_update_msg:partition_on_timestamp_node_id(Timestamp, MsgNodeId, MsgMap),
  % 检查分区后的消息映射 TMap 的大小是否大于 0，
  % 若大于 0 表示存在匹配的消息，返回 true；否则返回 false
  eh_system_util:size_map(TMap) > 0.




%% @doc 检查消息列表中是否存在已完成的消息
%% 
%% 该函数遍历消息列表，检查每个消息的键是否存在于已完成集合中。
%% 如果至少有一个消息键在已完成集合中，则返回true，否则返回false。
%%
%% @param UMsgList 消息列表，每个元素为{Key, Value}的元组
%% @param CompletedSet 已完成消息键的集合
%% @return boolean() - 如果存在已完成的消息则返回true，否则返回false
check_set(UMsgList, CompletedSet) ->
  lists:any(fun({UMsgKey, _}) -> eh_system_util:is_key_set(UMsgKey, CompletedSet) end, UMsgList).

%% @doc 解决消息冲突问题。
%% 该函数会比较消息列表中的消息和消息映射中的现有消息，根据节点 ID 判定是否存在冲突，
%% 若存在冲突则调用冲突解决器进行处理，并根据处理结果更新消息映射。
%% @spec resolve_conflict(MsgListTag :: term(), MsgMapTag :: term(), UMsgList :: list(), MsgMap :: map(), State :: #eh_system_state{}) -> {boolean(), map()}
%% @param MsgListTag 消息列表的标签，用于标识消息列表的类型。
%% @param MsgMapTag 消息映射的标签，用于标识消息映射的类型。
%% @param UMsgList 更新消息列表，包含待处理的消息信息。
%% @param MsgMap 消息映射，存储现有的消息信息。
%% @param #eh_system_state{app_config=AppConfig} 当前系统状态，包含应用配置信息。
%% @returns 返回一个元组 {IsValid, UpdatedMsgMap}，
%%          IsValid 表示消息是否有效，UpdatedMsgMap 为更新后的消息映射。
resolve_conflict(MsgListTag, MsgMapTag, UMsgList, MsgMap, #eh_system_state{app_config=AppConfig}) ->
  % 从更新消息列表中提取消息时间戳和消息节点 ID
  {MsgTimestamp, _, MsgNodeId, _} = eh_update_msg:get_msg_param(UMsgList),
  % 检查消息映射中是否存在与更新消息列表相关的消息，
  % 若存在则提取该消息的节点 ID、客户端 ID 和消息引用
  #eh_update_msg_data{node_id=EMsgNodeId, client_id=EClientId, msg_ref=EMsgRef} = eh_update_msg:exist_msg_list(UMsgList, MsgMap),
  % 根据消息映射中的节点 ID 以及其与消息列表中节点 ID 是否相等进行模式匹配
  case {EMsgNodeId, EMsgNodeId =:= MsgNodeId} of
    % 若消息映射中不存在对应的节点 ID
    {undefined, _} ->
      % 表示没有冲突，消息有效，返回原消息映射
      {true, MsgMap};
    % 若消息映射中的节点 ID 与消息列表中的节点 ID 相等
    {_, true}      ->
      % 表示没有冲突，消息有效，返回原消息映射
      {true, MsgMap};
    % 其他情况，即存在冲突
    {_, _}         ->
      % 从应用配置中获取写冲突解决器模块
      ConflictResolver = eh_system_config:get_write_conflict_resolver(AppConfig),
      % 调用冲突解决器的 resolve 函数，传入消息列表和消息映射的相关信息，得到解决后的节点 ID
      ResolvedNodeId = ConflictResolver:resolve({MsgListTag, MsgNodeId}, {MsgMapTag, EMsgNodeId}),
      % 判断解决后的节点 ID 是否与消息列表中的节点 ID 相等
      case ResolvedNodeId =:= MsgNodeId of
        % 若相等，说明消息列表中的消息胜出
        true  ->
          % 根据消息时间戳和消息映射中的节点 ID 对消息映射进行分区
          {TMsgMap, FMsgMap} = eh_update_msg:partition_on_timestamp_node_id(MsgTimestamp, EMsgNodeId, MsgMap),
          % 从应用配置中获取当前节点 ID
          NodeId = eh_system_config:get_node_id(AppConfig),
          % 判断消息映射中的节点 ID 是否为当前节点 ID
          case EMsgNodeId =:= NodeId of
            % 若是当前节点 ID
            true  ->
              % 从分区后的消息映射中获取消息列表
              [{_, EUMsgList}] = eh_update_msg:get_map_msg_list(TMsgMap),
              % 从消息列表中提取数据列表
              {_, _, _, _, DataList} = eh_update_msg:get_data_list(EUMsgList),
              % 调用查询处理模块向客户端回复错误信息，表示数据正在更新
              eh_query_handler:reply(EClientId, EMsgRef, eh_query_handler:error_being_updated(DataList));
            % 若不是当前节点 ID
            false ->
              ok
          end,
          % 返回消息有效，以及分区后剩余的消息映射
          {true, FMsgMap};
        % 若不相等，说明消息列表中的消息未胜出
        false ->
          % 返回消息无效，以及原消息映射
          {false, MsgMap}
      end
  end.


%% @doc 解决预更新消息的冲突问题。
%% 该函数会先尝试解决预消息数据中的冲突，若成功，再尝试解决消息数据中的冲突。
%% 最终根据冲突解决的结果返回一个布尔值和更新后的系统状态。
%% @spec pre_update_conflict_resolver(UMsgList :: list(), State :: #eh_system_state{}) -> {boolean(), #eh_system_state{}}
%% @param UMsgList 更新消息列表，包含待处理的消息信息。
%% @param #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData} 当前系统状态，包含预消息数据和消息数据。
%% @returns 返回一个元组 {Flag3, NewState}，
%%          Flag3 表示冲突是否解决成功，NewState 为更新后的系统状态。
pre_update_conflict_resolver(UMsgList, #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  % 调用 resolve_conflict 函数解决预消息数据中的冲突
  % ?EH_PRED_PRE_UPDATE 作为消息列表和消息映射的标签
  % 返回冲突解决结果 Flag1 和更新后的预消息数据 PreMsgData1
  {Flag1, PreMsgData1} = resolve_conflict(?EH_PRED_PRE_UPDATE, ?EH_PRED_PRE_UPDATE, UMsgList, PreMsgData, State),
  % 根据预消息数据冲突解决结果 Flag1 决定是否继续解决消息数据中的冲突
  Flag3 = case Flag1 of
            % 若预消息数据冲突解决成功
            true  ->
              % 调用 resolve_conflict 函数解决消息数据中的冲突
              % 消息列表标签为 ?EH_PRED_PRE_UPDATE，消息映射标签为 ?EH_SUCC_UPDATE
              % 只关注冲突解决结果 Flag2，忽略更新后的消息数据
              {Flag2, _} = resolve_conflict(?EH_PRED_PRE_UPDATE, ?EH_SUCC_UPDATE, UMsgList, MsgData, State),
              Flag2;
            % 若预消息数据冲突解决失败
            false ->
              % 直接返回 false，表示整体冲突解决失败
              false
          end,
  % 返回最终的冲突解决结果和更新后的系统状态
  % 系统状态中的预消息数据更新为 PreMsgData1
  {Flag3, State#eh_system_state{pre_msg_data=PreMsgData1}}.


%% @doc 解决更新消息的冲突问题。
%% 该函数调用 `resolve_conflict` 函数处理更新消息与预消息数据之间的冲突，
%% 并根据冲突解决结果更新系统状态中的预消息数据。
%% @spec update_conflict_resolver(UMsgList :: list(), State :: #eh_system_state{}) -> {boolean(), #eh_system_state{}}
%% @param UMsgList 更新消息列表，包含待处理的消息信息。
%% @param #eh_system_state{pre_msg_data=PreMsgData} 当前系统状态，包含预消息数据。
%% @returns 返回一个元组 {Flag1, NewState}，
%%          Flag1 表示冲突是否解决成功，NewState 为更新后的系统状态。
update_conflict_resolver(UMsgList, #eh_system_state{pre_msg_data=PreMsgData}=State) ->
  % 调用 resolve_conflict 函数解决更新消息与预消息数据之间的冲突
  % ?EH_SUCC_UPDATE 作为消息列表的标签，?EH_PRED_PRE_UPDATE 作为消息映射的标签
  % 返回冲突解决结果 Flag1 和更新后的预消息数据 PreMsgData1
  {Flag1, PreMsgData1} = resolve_conflict(?EH_SUCC_UPDATE, ?EH_PRED_PRE_UPDATE, UMsgList, PreMsgData, State),
  % 返回冲突解决结果和更新后的系统状态
  % 系统状态中的预消息数据更新为 PreMsgData1
  {Flag1, State#eh_system_state{pre_msg_data=PreMsgData1}}.










