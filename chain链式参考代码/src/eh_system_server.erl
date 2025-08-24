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

-module(eh_system_server).

-behavior(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlang_craq.hrl").

-define(SERVER, ?EH_SYSTEM_SERVER).

%% @doc 启动一个链接到 `gen_server' 进程的本地进程。
%% 该函数使用 `gen_server:start_link/4' 启动一个本地注册的进程，进程名为 ?SERVER。
%% @spec start_link(AppConfig :: term()) -> {ok, Pid} | ignore | {error, Error}
%%       其中 Pid 是新启动进程的进程 ID，Error 是启动失败时的错误信息。
%% @param AppConfig 应用配置信息，将传递给 `init/1' 函数。
start_link(AppConfig) ->
  % 启动一个本地注册的 gen_server 进程，使用当前模块作为回调模块，传递应用配置信息
  gen_server:start_link({local, ?SERVER}, ?MODULE, [AppConfig], []).

%% @doc 初始化 `gen_server' 进程的状态。
%% 该函数在 `gen_server' 进程启动时被调用，用于初始化进程的状态。
%% @spec init(Args :: [term()]) -> {ok, State}
%%       其中 State 是进程的初始状态。
%% @param Args 传递给 `start_link/1' 函数的参数列表，这里包含应用配置信息。
init([AppConfig]) ->
  % 创建一个初始的系统状态记录，包含应用配置信息
  State = #eh_system_state{app_config=AppConfig},
  % 返回初始化成功的结果和初始状态
  {ok, State}.

%% @doc 处理同步调用消息 {?EH_GET_DATA, {ObjectType, ObjectId}}。
%% 该函数从数据管理器中获取指定类型和 ID 的数据，并将结果返回给调用者。
%% @spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> {reply, Reply :: term(), NewState :: term()}
%% @param {?EH_GET_DATA, {ObjectType, ObjectId}} 请求消息，包含要获取数据的对象类型和 ID。
%% @param _From 调用者的信息，这里忽略。
%% @param #eh_system_state{app_config=AppConfig} 当前进程的状态，包含应用配置信息。
handle_call({?EH_GET_DATA, {ObjectType, ObjectId}},
            _From,
            #eh_system_state{app_config=AppConfig}=State) ->
  % 从应用配置中获取数据管理器模块
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  % 调用数据管理器的 get_data 函数获取指定类型和 ID 的数据
  Reply = ReplDataManager:get_data({ObjectType, ObjectId}),
  % 返回响应消息和当前状态
  {reply, Reply, State};

%% @doc 处理同步调用消息 ?EH_DATA_VIEW。
%% 该函数从数据管理器中获取数据视图，并将结果返回给调用者。
%% @spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> {reply, Reply :: term(), NewState :: term()}
%% @param ?EH_DATA_VIEW 请求消息，用于获取数据视图。
%% @param _From 调用者的信息，这里忽略。
%% @param #eh_system_state{app_config=AppConfig} 当前进程的状态，包含应用配置信息。
handle_call(?EH_DATA_VIEW, 
            _From, 
            #eh_system_state{app_config=AppConfig}=State) ->
  % 从应用配置中获取数据管理器模块
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  % 调用数据管理器的 data_view 函数获取数据视图
  Reply = ReplDataManager:data_view(),
  % 返回响应消息和当前状态
  {reply, Reply, State};

%% @doc 处理其他未明确处理的同步调用消息。
%% 该函数对未处理的消息返回默认响应 ok。
%% @spec handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: term()) -> {reply, Reply :: term(), NewState :: term()}
%% @param _Msg 未处理的请求消息，这里忽略。
%% @param _From 调用者的信息，这里忽略。
%% @param State 当前进程的状态。
handle_call(_Msg, _From, State) ->
  % 返回默认响应 ok 和当前状态
  {reply, ok, State}.


% 链的创建

% 接收 ?EH_SETUP_REPL 消息：
% 当 eh_system_server 收到 ?EH_SETUP_REPL 消息时，会触发链的创建过程。这个消息通常是在系统启动或节点加入集群时发送的。
handle_cast({?EH_SETUP_REPL, ReplRing}, 
            #eh_system_state{app_config=AppConfig}=State) ->
% 获取节点信息：
% 从 AppConfig 中获取当前节点的 ID (NodeId) 和节点顺序 (NodeOrder)。
  NodeId = eh_system_config:get_node_id(AppConfig),
  NodeOrder = eh_system_config:get_node_order(AppConfig),
%   计算链的顺序和邻居：
% 使用 eh_repl_ring:get_ordered_list_pred_succ/4 函数来计算当前节点在链中的位置，并确定其前驱 (Pred) 和后继 (Succ) 节点。这个函数返回链的有序列表 (ReplRing1)、链的顺序 (ReplRingOrder1)、前驱节点和后继节点。
  {ReplRing1, ReplRingOrder1, Pred, Succ} = eh_repl_ring:get_ordered_list_pred_succ(NodeId, ReplRing, ReplRing, NodeOrder),
%   设置故障检测器：
% 使用 FailureDetector:set/2 函数将当前节点及其链信息注册到故障检测器中，以便监控节点的状态。
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  % 获取数据管理器 (ReplDataManager):负责管理节点上的数据存储和复制逻辑。
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
%   设置故障检测器：将当前节点的 ID 和复制链 (ReplRing) 注册到故障检测器中。
  FailureDetector:set(NodeId, ReplRing),
  % 获取当前的时间戳，用于数据版本控制和一致性管理。
  {Timestamp, _} = ReplDataManager:timestamp(),
  % 将节点状态更新为 ready，表示该节点已准备好参与集群操作。
  NewState1 = eh_node_state:update_state_ready(State),
  % 更新系统状态，包括：repl_ring_order: 链的顺序。repl_ring: 当前的复制链。predecessor: 当前节点的前驱节点。successor: 当前节点的后继节点。timestamp: 当前的时间戳。
  NewState2 = NewState1#eh_system_state{repl_ring_order=ReplRingOrder1, repl_ring=ReplRing1, predecessor=Pred, successor=Succ, timestamp=Timestamp},
%   触发事件：
% 调用 event_state/3 函数，记录链创建的事件。
  event_state("setup_repl.99", NewState2, AppConfig),
  {noreply, NewState2};


% 添加节点


%% @doc 处理添加节点的异步消息 {?EH_ADD_NODE, {Node, NodeList, NodeOrderList}}。
%% 该函数根据接收到的消息更新节点信息，包括复制环、节点顺序、前驱和后继节点，
%% 并根据节点添加消息的有效性进行不同的处理。

%% @spec handle_cast(Request :: term(), State :: term()) -> {noreply, NewState :: term()}
%% @param {?EH_ADD_NODE, {Node, NodeList, NodeOrderList}} 请求消息，包含要添加的节点、节点列表和节点顺序列表。
%% @param #eh_system_state{app_config=AppConfig} 当前进程的状态，包含应用配置信息。
handle_cast({?EH_ADD_NODE, {Node, NodeList, NodeOrderList}}, 
            #eh_system_state{app_config=AppConfig}=State) ->

% 这里每个节点的 AppConfig 是独立的，通过 eh_system_config:get_failure_detector(AppConfig) 获取的故障检测器实例也相互独立。

  % 从应用配置中获取故障检测器模块
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  % 从应用配置中获取当前节点的顺序
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  % 从应用配置中获取当前节点的 ID
  NodeId = eh_system_config:get_node_id(AppConfig),
  % 计算更新后的节点列表、节点顺序列表、当前节点的前驱和后继节点
  {NodeList1, NodeOrderList1, Pred1, Succ1} = eh_repl_ring:get_ordered_list_pred_succ(NodeId, NodeList, NodeOrderList, NodeOrder),
  % 根据添加节点消息的有效性进行不同的处理
  NewState9 = case eh_node_timestamp:valid_add_node_msg(Node, State) of
                % 新节点处理步骤：
                ?EH_VALID_FOR_NEW      ->
                  % 处理快照请求，更新系统状态 【新节点需要获取集群的快照数据，这样才能和现有节点的数据状态保持一致。】
                  NewState1 = process_snapshot_request(NodeList1, NodeOrderList1, Succ1, State),
                  % 在故障检测器中设置新节点及其对应的节点列表 
                  FailureDetector:set(Node, NodeList1),
                  % 将节点状态更新为 transient
                  NewState2 = eh_node_state:update_state_transient(NewState1),
                  % 更新系统状态，包含新的节点顺序、节点列表、前驱和后继节点
                  NewState2#eh_system_state{repl_ring_order=NodeOrderList1, repl_ring=NodeList1, predecessor=Pred1, successor=Succ1};
                % 现有节点处理步骤：
                ?EH_VALID_FOR_EXISTING ->
                  % 在自身的故障检测器里注册新节点，让现有节点能监控新节点状态。
                  FailureDetector:set(Node),
                  % 更新系统状态，包含新的节点顺序、节点列表、前驱和后继节点
                  State#eh_system_state{repl_ring_order=NodeOrderList1, repl_ring=NodeList1, predecessor=Pred1, successor=Succ1};
                % 消息无效，保持当前状态不变
                _                      ->
                  State
              end,
  % 记录添加节点事件
  event_state("add_node.99", NewState9, AppConfig),
  % 返回不回复消息和更新后的状态
  {noreply, NewState9};




% 快照

%% @doc 处理快照消息 {?EH_SNAPSHOT, ...}。
%% 该函数接收快照消息，更新节点的复制环信息，生成新的快照数据，
%% 并将更新后的快照信息发送给发起快照请求的节点。
%% @spec handle_cast(Request :: term(), State :: term()) -> {noreply, NewState :: term()}
%% @param {?EH_SNAPSHOT, {Node, NodeList, NodeOrderList,  Ref, {Timestamp, Snapshot}}} 请求消息，
%%        包含发起快照请求的节点、节点列表、节点顺序列表、请求引用、时间戳和快照数据。
%% @param #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig} 当前进程的状态，
%%        包含预消息数据和应用配置信息。
handle_cast({?EH_SNAPSHOT, {Node, NodeList, NodeOrderList,  Ref, {Timestamp, Snapshot}}}, 
            #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) ->
  % 从应用配置中获取当前节点的 ID
  NodeId = eh_system_config:get_node_id(AppConfig),
  % 从应用配置中获取当前节点的顺序
  NodeOrder = eh_system_config:get_node_order(AppConfig),
  % 计算更新后的节点列表、节点顺序列表、当前节点的前驱和后继节点
  {NodeList1, NodeOrderList1, Pred1, Succ1} = eh_repl_ring:get_ordered_list_pred_succ(NodeId, NodeList, NodeOrderList, NodeOrder),
  % 从应用配置中获取数据管理器模块
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  % 调用数据管理器的 snapshot 函数，根据传入的时间戳和快照数据生成新的快照
  Q0 = ReplDataManager:snapshot(Timestamp, Snapshot),
  % 过滤预消息数据，获取有效的头部节点 ID 对应的消息映射
  % 需要进行过滤，是因为，并非所有消息都需要当前节点处理，只有那些以当前节点为"有效头节点"(effective head node)的消息才需要处理
  PendingPreMsgMap = eh_update_msg:filter_effective_head_node_id(PreMsgData, State),
  % 向发起快照请求的节点发送更新快照的消息 （Ref 是为了确保消息的唯一性，避免重复处理）（Q0是新的快照数据，快照本身不包括待处理消息）（PendingPreMsgMap是待处理的消息,这些消息需要发送给发起快照请求的节点，以便其在更新快照后继续处理这些消息，保证数据的一致性和完整性。）
  gen_server:cast({?EH_SYSTEM_SERVER, Node}, {?EH_UPDATE_SNAPSHOT, {Ref, Q0, PendingPreMsgMap}}),
  % 更新系统状态，包含新的节点顺序、节点列表、前驱和后继节点
  NewState2 = State#eh_system_state{repl_ring_order=NodeOrderList1, repl_ring=NodeList1, predecessor=Pred1, successor=Succ1},
  % 记录快照事件
  event_state("snapshot.99", NewState2, AppConfig),
  % 返回 {noreply, NewState2}，表示不回复消息，且更新进程状态为 NewState2。
  {noreply, NewState2};



% 更新快照

%% @doc 处理更新快照的异步消息 {?EH_UPDATE_SNAPSHOT, ...}。
%% @spec handle_cast(Request :: term(), State :: term()) -> {noreply, NewState :: term()}
handle_cast({?EH_UPDATE_SNAPSHOT, {Ref, Q0, PendingPreMsgData}},  %请求消息包含请求引用、新的快照数据和待处理的预消息数据。
            #eh_system_state{snapshot_ref=Ref, app_config=AppConfig}=State) ->%当前进程的状态，包含快照引用和应用配置信息，且【快照引用需与消息中的引用匹配】。
  % 从应用配置中获取数据管理器模块
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  % 调用数据管理器的 update_snapshot 函数，更新快照数据
  ReplDataManager:update_snapshot(Q0),
  % 验证待处理的预消息数据的有效性
  % 若有效，则保留原始的待处理预消息数据
  % 若无效，则创建一个新的空映射
  PendingPreMsgData1 = case eh_node_timestamp:valid_pending_pre_msg_data(PendingPreMsgData, State) of
                         true  ->
                           PendingPreMsgData;
                         false ->
                           eh_system_util:new_map()
                       end,
  % 调用 eh_node_state:update_state_snapshot 函数，更新节点的快照状态
  NewState2 = eh_node_state:update_state_snapshot(State),
  % 更新系统状态，将验证后的待处理预消息数据存入 pending_pre_msg_data 字段
  NewState3 = NewState2#eh_system_state{pending_pre_msg_data=PendingPreMsgData1},
  % 记录更新快照事件
  event_state("update_snapshot.99", NewState3, AppConfig),
  % 返回不回复消息和更新后的状态
  {noreply, NewState3};
handle_cast({?EH_UPDATE_SNAPSHOT, _}, State) -> % 当消息中的引用与当前进程状态中的快照引用不匹配时。
%% @param {?EH_UPDATE_SNAPSHOT, _} 请求消息，包含更新快照相关信息，这里忽略具体内容。
  {noreply, State};

%% @doc 处理异步查询消息
%% 该函数根据节点的数据状态和预消息数据，对查询请求进行不同处理。
%% @spec handle_cast(Request :: term(), State :: term()) -> {noreply, NewState :: term()}
handle_cast({?EH_QUERY, {From, Ref, {ObjectType, ObjectId}}}, %请求消息，包含调用者信息、引用标识、要查询的对象类型和对象 ID。
            #eh_system_state{pre_msg_data=PreMsgData, app_config=AppConfig}=State) -> %当前进程的状态，包含预消息数据和应用配置信息。
  NodeId = eh_system_config:get_node_id(AppConfig),
  % 根据节点的数据状态进行不同处理
  State1 = case eh_node_state:data_state(State) of
             % 若节点未准备好处理数据操作
             ?EH_NOT_READY ->
               % 向调用者回复节点不可用的错误信息
               eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
               % 保持当前状态不变
               State;
             % 若节点已准备好处理数据操作
             _             ->  
               % 检查预消息数据中是否存在指定类型和 ID 的消息
               case eh_update_msg:exist_map_msg(ObjectType, ObjectId, PreMsgData) of
                 % 若不存在相关消息，说明消息最新，可以查询
                 undefined ->
                    % 调用查询处理器进行正常查询操作
                    eh_query_handler:query(ok, ObjectType, ObjectId, From, Ref, State);
                 % 若存在相关消息，获取消息所在节点的 ID（说明存在未完成的更新操作，需要先处理）
                 MsgNodeId -> 
                    % 从应用配置中获取查询处理器模块
                    QueryHandler = eh_system_config:get_query_handler(AppConfig),
                    % 调用查询处理器处理该查询请求
                    QueryHandler:process(ObjectType, ObjectId, MsgNodeId, From, Ref, State)
               end
           end,
  % 返回不回复消息和处理后的状态
  {noreply, State1};

%% @doc 处理异步查询消息 {?EH_QUERY_AQ, {ObjectType, ObjectId, From, Ref}}。
%% 该函数根据节点的数据状态和前驱节点信息，对查询请求进行不同处理。
%% 例如：当节点处于未就绪状态（如刚启动、正在进行数据同步或故障恢复）
%% 
%% @spec handle_cast(Request :: term(), State :: term()) -> {noreply, NewState :: term()}
%% @param {?EH_QUERY_AQ, {ObjectType, ObjectId, From, Ref}} 请求消息，包含要查询的对象类型、对象 ID、调用者信息和引用标识。
%% @param #eh_system_state{predecessor=Pred, app_config=AppConfig} 当前进程的状态，包含前驱节点信息和应用配置信息。
%% @returns 返回不回复消息和处理后的状态。
handle_cast({?EH_QUERY_AQ, {ObjectType, ObjectId, From, Ref}},
            #eh_system_state{predecessor=Pred, app_config=AppConfig}=State) ->
  NodeId = eh_system_config:get_node_id(AppConfig),
  State1 = case eh_node_state:data_state(State) of %% 节点状态
             ?EH_NOT_READY ->
               % 根据前驱节点信息进行不同处理
               case Pred of
                 % 若没有前驱节点
                 undefined ->
                   % 向调用者回复节点不可用的错误信息
                   eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
                   % 保持当前状态不变
                   State;
                 % 若存在前驱节点
                 Other     ->
                   % 调用查询处理器处理尾节点查询操作（函数内：将查询转发给尾结点）
                   eh_query_handler:process_tail(ObjectType, ObjectId, Other, From, Ref, State)
               end;
             % 若节点已准备好处理数据操作
             _             ->
               % 调用查询处理器进行正常查询操作
               eh_query_handler:query(ok, ObjectType, ObjectId, From, Ref, State)
           end,
  % 返回不回复消息和处理后的状态
  {noreply, State1};



% 更新
% 当节点收到 ?EH_UPDATE 消息时，启动预写阶段
% 预写消息通过 send_pre_update_msg 函数发送给后继节点
handle_cast({?EH_UPDATE, {From, Ref, ObjectList}}, %更新消息，包含来源进程，请求引用，对象列表
            #eh_system_state{timestamp=Timestamp, 
			     successor=Succ, 
			     completed_set=CompletedSet, 
			     app_config=AppConfig}=State) -> %当前系统状态
  NodeId = eh_system_config:get_node_id(AppConfig),
  NewState9 = case eh_node_state:client_state(State) of %获取到根据节点状态所判断出的客户端状态。（函数内：若节点就绪 认为客户端就绪，若节点未 认为客户端未）
                ?EH_NOT_READY ->
                  eh_query_handler:reply(From, Ref, eh_query_handler:error_node_unavailable(NodeId)),
                  State; %若节点未就绪，则返回错误节点未就绪
                _             -> %否则
                  Timestamp1 = Timestamp+1, % +1 更新时间戳
                  {NodeId, UpdateList} = lists:keyfind(NodeId, 1, ObjectList), %从对象列表中查找当前节点 ID 对应的更新列表
                  % 根据更新列表、时间戳、调用者信息、节点 ID 和引用标识生成更新消息列表
                  UMsgList = eh_update_msg:get_msg(UpdateList, 
                                                   Timestamp1,
                                                   From,
                                                   NodeId,
                                                   Ref),
                  % 将新的时间戳更新到系统中
                  NewState1 = eh_node_timestamp:update_state_timestamp(Timestamp1, State),
                  case Succ of % 根据后继节点情况进行不同处理
                    undefined -> %没有后继节点（说明是尾结点）
                      reply_to_client(fun eh_persist_data:persist_data/2, UMsgList, CompletedSet, NewState1);% 直接将更新消息回复给客户端，并持久化数据
                    _         -> 
                      send_pre_update_msg(fun eh_persist_data:no_persist_data/2, UMsgList, CompletedSet, NewState1) % 发送预写更新消息给后继节点，不进行数据持久化
                  end                            
              end,
  event_state("update.99", NewState9, AppConfig),%更新记录事件
  {noreply, NewState9};

% 当节点收到(从前驱节点发来的)预写消息 ?EH_PRED_PRE_UPDATE 时，继续传播给后继节点：
handle_cast({?EH_PRED_PRE_UPDATE, {UMsgList, CompletedSet}}, State) -> %  预更新消息，包括更新消息列表和已完成集合。   和 系统状态
  % 调用 process_msg 函数处理预写消息
  NewState1 = process_msg(?EH_PRED_PRE_UPDATE,
                          % 验证预写更新消息的有效性
                          fun eh_node_timestamp:valid_pre_update_msg/2,
                          % 处理返回消息，调用 send_update_msg 函数
                          fun send_update_msg/4,
                          % 持久化返回消息数据
                          fun eh_persist_data:persist_data/2,
                          % 处理有效消息，调用 send_pre_update_msg 函数
                          fun send_pre_update_msg/4,
                          % 不持久化有效消息数据
                          fun eh_persist_data:no_persist_data/2,
                          UMsgList,
                          CompletedSet,
                          State),
  % 返回不回复消息和处理后的状态
  {noreply, NewState1};

% 处理更新确认消息
handle_cast({?EH_SUCC_UPDATE, {UMsgList, CompletedSet}}, State) ->
  % 调用 process_msg 函数处理更新确认消息
  NewState1 = process_msg(?EH_SUCC_UPDATE,
                          % 验证更新消息的有效性
                          fun eh_node_timestamp:valid_update_msg/2,
                          % 处理返回消息，调用 reply_to_client 函数
                          fun reply_to_client/4,
                          % 持久化返回消息数据
                          fun eh_persist_data:persist_data/2,
                          % 处理有效消息，调用 send_update_msg 函数
                          fun send_update_msg/4,
                          % 持久化有效消息数据
                          fun eh_persist_data:persist_data/2,
                          UMsgList,
                          CompletedSet,
                          State),
  {noreply, NewState1};

% 处理停止消息，记录停止事件并停止进程。
handle_cast({stop, Reason}, #eh_system_state{app_config=AppConfig}=State) ->
  event_data("stop", status, stopped, AppConfig),  % 记录停止事件
  {stop, Reason, State}; % 停止进程，返回停止原因和当前状态

% 其他info消息，不做处理。
handle_cast(_Msg, State) ->
  {noreply, State}.


% 这是 处理系统内部消息(非客户端请求)的回调函数

%% @doc 处理异步信息消息。
%% 该函数接收异步消息，通过故障检测器检测消息，根据检测结果处理节点下线事件，
%% 并更新系统状态，最后返回不回复消息和更新后的状态。
%% @spec handle_info(Msg :: term(), State :: #eh_system_state{}) -> {noreply, NewState :: #eh_system_state{}}


%% @returns 返回不回复消息和处理后的状态。
handle_info(Msg, %% @param Msg 接收到的异步消息。
            #eh_system_state{repl_ring=ReplRing, 
			     repl_ring_order=ReplRingOrder, 
			     predecessor=Pred, 
			     successor=Succ, 
			     pre_msg_data=PreMsgData, 
			     msg_data=MsgData, 
			     app_config=AppConfig}=State) -> %% @param #eh_system_state{...} 当前进程的状态，包含复制环、复制环顺序、前驱节点、后继节点、预消息数据、消息数据和应用配置信息。
  % 从应用配置中获取故障检测器模块
  FailureDetector = eh_system_config:get_failure_detector(AppConfig),
  % 根据故障检测器的检测结果进行不同处理
  NewState9 = case FailureDetector:detect(Msg) of
                % 若检测到节点下线事件(失效)
                {?EH_NODEDOWN, DownNode} ->
                  % 记录节点下线事件
                  event_data("failure", node_down, eh_system_util:get_node_name(DownNode), AppConfig),
                  % 从应用配置中获取当前节点的 ID
                  NodeId = eh_system_config:get_node_id(AppConfig),
                  % 从应用配置中获取当前节点的顺序
                  NodeOrder = eh_system_config:get_node_order(AppConfig),
                  % 从复制环中移除下线节点，得到新的复制环
                  {NewReplRing, _} = eh_repl_ring:drop(DownNode, ReplRing, ReplRingOrder, NodeOrder),
                  % 计算新的前驱节点
                  NewPred = eh_repl_ring:predecessor(NodeId, NewReplRing, ReplRingOrder, NodeOrder),
                  % 计算新的后继节点
                  NewSucc = eh_repl_ring:successor(NodeId, NewReplRing, ReplRingOrder, NodeOrder),
                  % 更新系统状态，包含新的复制环、前驱节点和后继节点
                  NewState1 = State#eh_system_state{repl_ring=NewReplRing, predecessor=NewPred, successor=NewSucc},
                  % 根据新的后继节点、快照状态以及下线节点与前后继节点的关系进行不同处理
                  NewState3 = case {NewSucc, eh_node_state:snapshot_state(NewState1), Succ =:= DownNode, Pred =:= DownNode} of
                                % 若新的后继节点为 undefined(没有)且快照状态就绪,则处理节点下线消息
                                {undefined, ?EH_READY, _, _} ->
                                  process_down_msg(NewState1);
                                % 若快照状态未就绪且后继节点下线,则发起快照请求
                                {_, ?EH_NOT_READY, true, _} ->
                                  process_snapshot_request(NewReplRing, ReplRingOrder, NewSucc, NewState1);
                                % 若快照状态就绪且后继节点下线，前驱节点未下线
                                {_, ?EH_READY, true, false} ->
                                  % 发送预写更新消息给新的后继节点
                                  send_down_msg(?EH_PRED_PRE_UPDATE,
                                                fun eh_persist_data:no_persist_data/2,
                                                fun send_pre_update_msg/4,
                                                fun eh_persist_data:persist_data/2,
                                                fun send_update_msg/4,
                                                PreMsgData,
                                                NewState1);
                                % 若快照状态就绪且前驱节点下线，后继节点未下线
                                {_, ?EH_READY, false, true} ->
                                  % 发送更新确认消息给新的前驱节点
                                  send_down_msg(?EH_SUCC_UPDATE,
                                                fun eh_persist_data:no_persist_data/2,
                                                fun send_update_msg/4,
						fun eh_persist_data:no_persist_data/2,
					        fun reply_to_client/4,
                                                MsgData,
                                                NewState1);
                                % 其他情况，保持状态不变
                                {_, _, _, _} ->
                                  NewState1  
                              end,
                  % 记录故障处理事件
                  event_state("failure.99", NewState3, AppConfig),
                  NewState3;
                % 若不是节点下线事件，保持当前状态不变
                _                        ->
                  State 
              end,
  % 返回不回复消息和处理后的状态
  {noreply, NewState9}.


%% @doc 处理代码版本变更。
%% 当系统进行代码热更新时，该函数会被调用，用于将进程状态从旧版本迁移到新版本。
%% 在当前实现中，不进行任何状态迁移操作，直接返回原状态。
%% @spec code_change(OldVsn :: term(), State :: term(), Extra :: term()) -> {ok, NewState :: term()}
%% @param _OldVsn 旧的代码版本号，这里忽略。
%% @param State 当前进程的状态。
%% @param _Extra 额外的参数，这里忽略。
%% @returns 返回 {ok, State}，表示状态迁移成功，且新状态与原状态相同。
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%% @doc 处理进程终止操作。
%% 当 `gen_server` 进程终止时，该函数会被调用，用于执行一些清理工作。
%% 在当前实现中，不进行任何特殊的清理操作，直接返回 `ok`。
%% @spec terminate(Reason :: term(), State :: term()) -> ok
%% @param _Reason 进程终止的原因，这里忽略。
%% @param _State 当前进程的状态，这里忽略。
%% @returns 返回 `ok`，表示终止操作完成。
terminate(_Reason, _State) ->
  ok.

%% @doc 记录状态事件。
%% 该函数调用 `eh_event` 模块的 `state/4` 函数，记录与节点状态相关的事件。
%% @spec event_state(Msg :: string(), State :: term(), AppConfig :: term()) -> term()
%% @param Msg 事件消息，用于描述事件的具体内容。
%% @param State 当前进程的状态。
%% @param AppConfig 应用配置信息。
%% @returns 返回 `eh_event:state/4` 函数的执行结果。
event_state(Msg, State, AppConfig) ->
  eh_event:state(?MODULE, Msg, State, AppConfig).

%% @doc 记录消息事件。
%% 该函数调用 `eh_event` 模块的 `message/4` 函数，记录与消息处理相关的事件。
%% @spec event_message(Msg :: string(), MsgList :: term(), CompletedSet :: term(), AppConfig :: term()) -> term()
%% @param Msg 事件消息，用于描述事件的具体内容。
%% @param MsgList 消息列表，包含处理的消息信息。
%% @param CompletedSet 已完成的消息集合。
%% @param AppConfig 应用配置信息。
%% @returns 返回 `eh_event:message/4` 函数的执行结果。
event_message(Msg, MsgList, CompletedSet, AppConfig) ->
  eh_event:message(?MODULE, Msg, {MsgList, CompletedSet}, AppConfig).

%% @doc 记录与数据操作相关的事件。
%% 
%% @spec event_data(Msg :: string(), DataMsg :: term(), Data :: term(), AppConfig :: term()) -> term()
%% @param Msg 事件消息，用于描述事件的具体内容。
%% @param DataMsg 数据相关的消息，用于描述数据操作的内容。
%% @param Data 具体的数据。
%% @param AppConfig 应用配置信息。
%% @returns 返回 `eh_event:data/4` 函数的执行结果。
event_data(Msg, DataMsg, Data, AppConfig) ->
  eh_event:data(?MODULE, Msg, {DataMsg, Data}, AppConfig).



%% @doc 将更新消息回复给客户端，并更新系统状态。
%% 该函数首先调用持久化函数将更新消息持久化，然后从更新消息列表中提取客户端 ID、引用标识和数据列表，
%% 接着将更新后的数据列表回复给客户端，最后更新系统状态以记录客户端回复信息。
%% @spec reply_to_client(PersistFun :: fun((list(), term()) -> term()), UMsgList :: list(), _CompletedSet :: term(), State :: term()) -> term()
%% @param PersistFun 持久化函数，用于将更新消息持久化到存储中。
%% @param UMsgList 更新消息列表，包含客户端 ID、引用标识和更新的数据列表等信息。
%% @param _CompletedSet 已完成的消息集合，在本函数中未使用。
%% @param State 当前系统状态。
%% @returns 返回更新后的系统状态。
reply_to_client(PersistFun,
                UMsgList,
                _CompletedSet,
                State) -> 
  % 调用持久化函数，将更新消息列表 UMsgList 持久化到存储中，并更新系统状态
  State1 = PersistFun(UMsgList, State), 
  % 从更新消息列表 UMsgList 中提取客户端 ID、引用标识和数据列表
  {_, ClientId, _, Ref, DataList} = eh_update_msg:get_data_list(UMsgList),
  % 将更新后的数据列表 DataList 回复给客户端，使用客户端 ID 和引用标识确保消息准确送达
  eh_query_handler:reply(ClientId, Ref, eh_query_handler:updated(DataList)),
  % 更新系统状态，记录客户端回复信息，以便后续跟踪和处理
  eh_node_timestamp:update_state_client_reply(UMsgList, State1).

%% @doc 发送消息给目标节点。
%% 该函数根据传入的消息标签确定目标节点，将更新消息列表进行持久化处理，
%% 更新节点状态，然后异步发送消息给目标节点，最后返回更新后的节点状态。
%% @spec send_msg(Tag :: atom(), PersistFun :: fun((list(), term()) -> term()), UMsgList :: list(), CompletedSet :: term(), State :: #eh_system_state{}) -> #eh_system_state{}
%% @param Tag 消息标签，用于区分消息类型，目前支持 ?EH_PRED_PRE_UPDATE 和 ?EH_SUCC_UPDATE。
%% @param PersistFun 持久化函数，用于将更新消息列表持久化到存储中。
%% @param UMsgList 更新消息列表，包含需要发送的消息信息。
%% @param CompletedSet 已完成的消息集合，记录已经处理过的消息。
%% @param #eh_system_state{predecessor=Pred, successor=Succ} 当前系统状态，包含前驱节点和后继节点信息。
%% @returns 返回更新后的系统状态。
send_msg(Tag, 
         PersistFun,
         UMsgList,
         CompletedSet,
         #eh_system_state{predecessor=Pred, successor=Succ}=State) ->
  % 调用持久化函数，将更新消息列表 UMsgList 持久化到存储中，并更新系统状态
  State1 = PersistFun(UMsgList, State), 
  % 根据消息标签 Tag 和更新消息列表 UMsgList 更新节点的时间戳和消息状态
  State2 = eh_node_timestamp:update_state_new_msg(Tag, UMsgList, State1), 
  % 根据消息标签确定消息的目标节点
  Dest = case Tag of
           % 预写更新消息，通常由前驱节点发送给后继节点
           ?EH_PRED_PRE_UPDATE ->
             Succ;
           % 更新确认消息，一般由后继节点发送给前驱节点
           ?EH_SUCC_UPDATE     ->
             Pred
         end,
  % 异步发送消息给目标节点，消息内容包含消息标签、更新消息列表和已完成的消息集合
  gen_server:cast({?EH_SYSTEM_SERVER, Dest}, {Tag, {UMsgList, CompletedSet}}),
  % 返回更新后的系统状态
  State2.  


% 预写消息通过 send_pre_update_msg 函数发送给后继节点：
%% @doc 发送预写更新消息给后继节点。
%% 该函数调用 `send_msg/5` 函数，将预写更新消息标签 `?EH_PRED_PRE_UPDATE` 以及相关参数传递给它，
%% 以实现将预写更新消息发送给后继节点的功能。
%% @spec send_pre_update_msg(PersistFun :: fun((list(), term()) -> term()), UMsgList :: list(), CompletedSet :: term(), State :: #eh_system_state{}) -> #eh_system_state{}
%% @param PersistFun 持久化函数，用于将更新消息列表持久化到存储中。
%% @param UMsgList 更新消息列表，包含需要发送的预写更新消息信息。
%% @param CompletedSet 已完成的消息集合，记录已经处理过的消息。
%% @param State 当前系统状态，包含前驱节点、后继节点等信息。
%% @returns 返回更新后的系统状态。
send_pre_update_msg(PersistFun,
                    UMsgList,
                    CompletedSet,
                    State) ->
  % 调用 send_msg 函数，发送预写更新消息给后继节点
  send_msg(?EH_PRED_PRE_UPDATE, PersistFun, UMsgList, CompletedSet, State).
 
%% @doc 发送更新确认消息给前驱节点。
%% 该函数调用 `send_msg/5` 函数，将更新确认消息标签 `?EH_SUCC_UPDATE` 以及相关参数传递给它，
%% 以实现将更新确认消息发送给前驱节点的功能。
%% @spec send_update_msg(PersistFun :: fun((list(), term()) -> term()), UMsgList :: list(), CompletedSet :: term(), State :: #eh_system_state{}) -> #eh_system_state{}
%% @param PersistFun 持久化函数，用于将更新消息列表持久化到存储中。
%% @param UMsgList 更新消息列表，包含需要发送的更新确认消息信息。
%% @param CompletedSet 已完成的消息集合，记录已经处理过的消息。
%% @param State 当前系统状态，包含前驱节点、后继节点等信息。
%% @returns 返回更新后的系统状态。
send_update_msg(PersistFun,
                UMsgList,
                CompletedSet,
		State) ->
  % 调用 send_msg 函数，发送更新确认消息给前驱节点
  send_msg(?EH_SUCC_UPDATE, PersistFun, UMsgList, CompletedSet, State).





%% @doc 处理更新消息列表。根据节点状态和消息有效性，对消息进行不同处理，并记录相应事件。
%% @spec process_msg(Tag :: atom(), ValidateMsgFun :: fun((list(), term()) -> term()), ReturnedMsgFun :: fun((fun((list(), term()) -> term()), list(), term(), term()) -> term()),
%%                   ReturnedMsgPersistFun :: fun((list(), term()) -> term()), ValidMsgFun :: fun((fun((list(), term()) -> term()), list(), term(), term()) -> term()),
%%                   ValidMsgPersistFun :: fun((list(), term()) -> term()), UMsgList :: list(), CompletedSet :: term(), State :: #eh_system_state{}) -> term()
%% @param Tag 消息标签，用于标识消息类型，不同类型消息有不同处理逻辑。
%% @param ValidateMsgFun 验证消息有效性的函数，接收消息列表和当前状态，返回验证结果和更新后的状态。
%% @param ReturnedMsgFun 处理返回消息的函数，当消息为头部或尾部消息时调用。
%% @param ReturnedMsgPersistFun 处理返回消息时使用的持久化函数。
%% @param ValidMsgFun 处理有效消息的函数，当消息为环消息时调用。
%% @param ValidMsgPersistFun 处理有效消息时使用的持久化函数。
%% @param UMsgList 更新消息列表，包含待处理的消息。
%% @param CompletedSet 已完成的消息集合，用于避免重复处理消息。
%% @param #eh_system_state{app_config=AppConfig} 当前系统状态，包含应用配置信息。
%% @returns 返回处理后的系统状态。
process_msg(Tag,
            ValidateMsgFun, 
            ReturnedMsgFun,
            ReturnedMsgPersistFun,
            ValidMsgFun,
            ValidMsgPersistFun,
            UMsgList,
            CompletedSet,
            #eh_system_state{app_config=AppConfig}=State) ->
  % 将消息标签转换为字符串，用于记录事件
  DisplayTag = eh_system_util:display_atom_to_list(Tag),
  % 从更新消息列表中获取消息的时间戳
  {MsgTimestamp, _, _, _} = eh_update_msg:get_msg_param(UMsgList),
  % 根据节点的消息状态进行不同处理
  NewState8 = case eh_node_state:msg_state(State) of
                % 若节点未准备好处理消息
                ?EH_NOT_READY ->
                  % 记录无效消息事件
                  event_message(DisplayTag++".invalid_msg", UMsgList, CompletedSet, AppConfig),
                  % 保持当前状态不变
                  State;
                % 若节点已准备好处理消息
                _             ->
                  % 调用验证函数验证消息的有效性
                  case ValidateMsgFun(UMsgList, State) of  %是函数参数,会根据不同的传参,使用不同的函数
                    % 若消息无效，为重复消息
                    {false, _, NewState1}           ->
                      % 记录重复消息事件
                      event_message(DisplayTag++".duplicate_msg", UMsgList, CompletedSet, AppConfig),
                      % 返回更新后的状态
                      NewState1;
                    % 若消息有效，且为头部消息
                    {true, ?EH_HEAD_MSG, NewState1} ->
                      % 记录有效头部消息事件
                      event_message(DisplayTag++".valid_head_msg", UMsgList, CompletedSet, AppConfig),
                      % 更新节点状态的时间戳
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      % 更新节点状态的已完成消息集合
                      NewState3 = eh_node_timestamp:update_state_completed_set(CompletedSet, NewState2),
                      % 调用处理返回消息的函数
                      ReturnedMsgFun(ReturnedMsgPersistFun, UMsgList, CompletedSet, NewState3);
                    % 若消息有效，且为尾部消息
                    {true, ?EH_TAIL_MSG, NewState1} ->
                      % 记录有效尾部消息事件
                      event_message(DisplayTag++".valid_tail_msg", UMsgList, CompletedSet, AppConfig),
                      % 更新节点状态的时间戳
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      % 更新节点状态的消息数据
                      NewState3 = eh_node_timestamp:update_state_msg_data(CompletedSet, NewState2),
                      % 调用处理返回消息的函数
                      ReturnedMsgFun(ReturnedMsgPersistFun, UMsgList, CompletedSet, NewState3);
                    % 若消息有效，且为环消息
                    {true, _, NewState1}            ->
                      % 记录有效环消息事件
                      event_message(DisplayTag++".valid_ring_msg", UMsgList, CompletedSet, AppConfig),
                      % 更新节点状态的时间戳
                      NewState2 = eh_node_timestamp:update_state_timestamp(MsgTimestamp, NewState1),
                      % 更新节点状态的消息数据
                      NewState3 = eh_node_timestamp:update_state_msg_data(CompletedSet, NewState2),
                      % 调用处理有效消息的函数
                      ValidMsgFun(ValidMsgPersistFun, UMsgList, CompletedSet, NewState3)
                  end
              end,
  % 记录消息处理完成事件
  event_state(DisplayTag++".99", NewState8, AppConfig),
  % 返回处理后的系统状态
  NewState8.


%% @doc 处理快照请求，向指定节点发送快照请求消息，并更新系统状态中的快照引用。
%% @spec process_snapshot_request(NodeList :: list(), NodeOrderList :: list(), Succ :: term(), State :: #eh_system_state{}) -> #eh_system_state{}
%% @param NodeList 节点列表，包含系统中的所有节点信息。
%% @param NodeOrderList 节点顺序列表，记录节点的排列顺序。
%% @param Succ 后继节点，快照请求将发送给该节点。
%% @param #eh_system_state{app_config=AppConfig} 当前系统状态，包含应用配置信息。
%% @returns 返回更新后的系统状态，其中包含新的快照引用。
process_snapshot_request(NodeList, NodeOrderList, Succ, #eh_system_state{app_config=AppConfig}=State) ->
  % 从应用配置中获取当前节点的 ID
  NodeId = eh_system_config:get_node_id(AppConfig),
  % 从应用配置中获取唯一 ID 生成器模块
  UniqueIdGenerator = eh_system_config:get_unique_id_generator(AppConfig),
  % 从应用配置中获取数据复制管理器模块
  ReplDataManager = eh_system_config:get_repl_data_manager(AppConfig),
  % 调用唯一 ID 生成器生成一个唯一的快照引用
  SnapshotRef = UniqueIdGenerator:unique_id(),
  % 从数据复制管理器获取当前时间戳和快照数据
  {Timestamp, Snapshot} = ReplDataManager:timestamp(),
  % 异步发送快照请求消息给后继节点
  gen_server:cast({?EH_SYSTEM_SERVER, Succ}, {?EH_SNAPSHOT, {NodeId, NodeList, NodeOrderList, SnapshotRef, {Timestamp, Snapshot}}}),
  % 更新系统状态中的快照引用并返回
  State#eh_system_state{snapshot_ref=SnapshotRef}.

%% @doc 根据指定的持久化函数处理消息映射中的消息列表，将消息回复给客户端。
%% @spec process_down_msg(PersistFun :: fun((list(), term()) -> term()), MsgMap :: map(), State :: term()) -> term()
%% @param PersistFun 持久化函数，用于将消息持久化到存储中。
%% @param MsgMap 消息映射，包含多个消息列表。
%% @param State 当前系统状态。
%% @returns 返回处理完所有消息后的系统状态。
process_down_msg(PersistFun,
                 MsgMap,
                 State) ->
  % 从消息映射中获取消息列表
  MsgList = eh_update_msg:get_map_msg_list(MsgMap),
  % 遍历消息列表，依次将消息回复给客户端，并更新系统状态
  lists:foldl(fun({_, UMsgList}, StateX) -> reply_to_client(PersistFun, UMsgList, undefined, StateX) end, State, MsgList).

%% @doc 处理节点下线时的消息，分别处理预消息数据和消息数据，并重置相关状态。
%% @spec process_down_msg(State :: #eh_system_state{}) -> #eh_system_state{}
%% @param #eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData} 当前系统状态，包含预消息数据和消息数据。
%% @returns 返回处理完节点下线消息后的系统状态，预消息数据、消息数据和已完成集合将被重置。
process_down_msg(#eh_system_state{pre_msg_data=PreMsgData, msg_data=MsgData}=State) ->
  % 处理预消息数据，使用持久化函数进行持久化
  State1 = process_down_msg(fun eh_persist_data:persist_data/2, PreMsgData, State),
  % 处理消息数据，不进行持久化
  State2 = process_down_msg(fun eh_persist_data:no_persist_data/2, MsgData, State1),
  % 重置系统状态中的预消息数据、消息数据和已完成集合
  State2#eh_system_state{pre_msg_data=eh_system_util:new_map(), msg_data=eh_system_util:new_map(), completed_set=eh_system_util:new_set()}.

%% @doc 根据消息标签和消息状态，选择合适的函数处理单个更新消息列表。
%% @spec send_down_msg(Tag :: atom(), PersistRingFun :: fun((list(), term()) -> term()), RingFun :: fun((fun((list(), term()) -> term()), list(), term(), term()) -> term()),
%%                     PersistReturnFun :: fun((list(), term()) -> term()), ReturnFun :: fun((fun((list(), term()) -> term()), list(), term(), term()) -> term()),
%%                     UMsgList :: list(), CompletedSet :: term(), State :: term()) -> term()
%% @param Tag 消息标签，用于区分消息类型。
%% @param PersistRingFun 环消息持久化函数，用于持久化环消息。
%% @param RingFun 环消息处理函数，用于处理环消息。
%% @param PersistReturnFun 返回消息持久化函数，用于持久化返回消息。
%% @param ReturnFun 返回消息处理函数，用于处理返回消息。
%% @param UMsgList 更新消息列表，包含待处理的消息。
%% @param CompletedSet 已完成的消息集合，记录已经处理过的消息。
%% @param State 当前系统状态。
%% @returns 返回处理完消息后的系统状态。
send_down_msg(Tag,
              PersistRingFun,
              RingFun,
              PersistReturnFun,
              ReturnFun,
              UMsgList,
              CompletedSet,
              State) ->
  % 根据消息标签和消息状态选择处理方式
  case {Tag, eh_node_timestamp:msg_status(UMsgList, State)} of
    % 若消息标签为 ?EH_PRED_PRE_UPDATE 且消息为尾部消息
    {?EH_PRED_PRE_UPDATE, ?EH_TAIL_MSG} ->
      % 调用返回消息处理函数处理消息
      ReturnFun(PersistReturnFun, UMsgList, CompletedSet, State);
    % 若消息标签为 ?EH_SUCC_UPDATE 且消息为头部消息
    {?EH_SUCC_UPDATE, ?EH_HEAD_MSG}     ->
      % 调用返回消息处理函数处理消息
      ReturnFun(PersistReturnFun, UMsgList, CompletedSet, State);
    % 其他情况
    {_, _}                              ->
      % 调用环消息处理函数处理消息
      RingFun(PersistRingFun, UMsgList, CompletedSet, State)
  end.

%% @doc 根据消息标签和消息状态，处理消息映射中的所有更新消息列表。
%% @spec send_down_msg(Tag :: atom(), PersistRingFun :: fun((list(), term()) -> term()), RingFun :: fun((fun((list(), term()) -> term()), list(), term(), term()) -> term()),
%%                     PersistReturnFun :: fun((list(), term()) -> term()), ReturnFun :: fun((fun((list(), term()) -> term()), list(), term(), term()) -> term()),
%%                     MsgMap :: map(), State :: term()) -> term()
%% @param Tag 消息标签，用于区分消息类型。
%% @param PersistRingFun 环消息持久化函数，用于持久化环消息。
%% @param RingFun 环消息处理函数，用于处理环消息。
%% @param PersistReturnFun 返回消息持久化函数，用于持久化返回消息。
%% @param ReturnFun 返回消息处理函数，用于处理返回消息。
%% @param MsgMap 消息映射，包含多个消息列表。
%% @param State 当前系统状态。
%% @returns 返回处理完所有消息后的系统状态。
send_down_msg(Tag,
              PersistRingFun,
              RingFun,
              PersistReturnFun,
              ReturnFun,
              MsgMap,
              State) ->
  % 创建一个新的空已完成消息集合
  CompletedSet = eh_system_util:new_set(),
  % 从消息映射中获取消息列表
  MsgList = eh_update_msg:get_map_msg_list(MsgMap),
  % 遍历消息列表，依次处理每个消息列表，并更新系统状态
  lists:foldl(fun({_, UMsgList}, StateX) -> send_down_msg(Tag, PersistRingFun, RingFun, PersistReturnFun, ReturnFun, UMsgList, CompletedSet, StateX) end, State, MsgList).






                 




