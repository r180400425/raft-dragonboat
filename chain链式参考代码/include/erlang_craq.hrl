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

-define(STATUS_INACTIVE,           0).
-define(STATUS_ACTIVE,             1).

-define(READ_TIMEOUT,              500).
-define(UPDATE_TIMEOUT,            1000).

-define(EH_BAD_DATA,               eh_bad_data).

-define(EH_NODEDOWN,               nodedown).
-define(EH_BEING_UPDATED,          being_updated).
-define(EH_NODE_UNAVAILABLE,       node_unavailable).
-define(EH_UPDATED,                updated).

-define(EH_SORTED,                 sorted).
-define(EH_USER_DEFINED,           user_defined).

-define(EH_INVALID_MSG,            eh_invalid_msg).
-define(EH_VALID_FOR_EXISTING,     eh_valid_for_existing).
-define(EH_VALID_FOR_NEW,          eh_valid_for_new).
-define(EH_RING_MSG,               eh_ring_msg).
-define(EH_HEAD_MSG,               eh_head_msg).
-define(EH_TAIL_MSG,               eh_tail_msg).

-define(EH_SETUP_REPL,             eh_setup_repl).
-define(EH_ADD_NODE,               eh_add_node).
-define(EH_UPDATE,                 eh_update).
-define(EH_SUCC_UPDATE,            eh_succ_update).
-define(EH_PRED_PRE_UPDATE,        eh_pred_pre_update).
-define(EH_TIMESTAMP,              eh_timestamp).
-define(EH_QUERY,                  eh_query).
-define(EH_QUERY_AQ,               eh_query_aq).
-define(EH_SNAPSHOT,               eh_snapshot).
-define(EH_UPDATE_SNAPSHOT,        eh_update_snapshot).
-define(EH_DATA_VIEW,              eh_data_view).
-define(EH_CHECK_DATA,             eh_check_data).
-define(EH_GET_DATA,               eh_get_data).

-define(EH_NOT_READY,              eh_not_ready).
-define(EH_READY,                  eh_ready).
-define(EH_TRANSIENT,              eh_transient).
-define(EH_TRANSIENT_DU,           eh_transient_data_updated).
-define(EH_TRANSIENT_TU,           eh_transient_timestamp_updated).

-define(EH_SYSTEM_SERVER,          eh_system_server).
-define(EH_DATA_SERVER,            eh_data_server).

-define(GEN_EVENT,                 gen_event).
-define(LAGER_EVENT,               lager_event).

% 定义应用配置记录，包含节点、故障检测、数据管理等相关配置信息。
-record(eh_app_config,          {node_id                             :: atom(),% 节点的唯一标识符，通常为原子类型，用于在分布式系统中唯一标识一个节点。
                                 node_order                          :: ?EH_SORTED | ?EH_USER_DEFINED,% 节点顺序类型，?EH_SORTED 表示按排序顺序，?EH_USER_DEFINED 表示用户自定义顺序。
                                 failure_detector                    :: atom(),% 故障检测器模块名，原子类型，用于检测节点故障。
                                 repl_data_manager                   :: atom(),% 复制数据管理器模块名，原子类型，负责管理数据的复制操作。
                                 storage_data                        :: atom(),% 存储数据模块名，原子类型，用于处理数据的存储操作。
                                 write_conflict_resolver             :: atom(),% 写冲突解决器模块名，原子类型，用于解决写入数据时发生的冲突。
                                 unique_id_generator                 :: atom(),% 唯一 ID 生成器模块名，原子类型，用于生成唯一的标识符。
                                 query_handler                       :: atom(),% 查询处理器模块名，原子类型，负责处理查询请求。
				 event_logger                        :: ?GEN_EVENT | ?LAGER_EVENT,% 事件日志记录器类型，?GEN_EVENT 或 ?LAGER_EVENT，指定使用的日志记录方式。
				 data_checkpoint=0                   :: non_neg_integer(),% ？【解释1：数据检查点间隔，非负整数，表示每隔多少个操作创建一次数据检查点。默认值为 0。】
                                                                          % ？【 解释2：数据检查点，非负整数，表示数据的检查点版本】
				 data_dir                            :: string(),% 数据存储目录，字符串类型，指定数据文件存储的路径。
				 file_repl_data_suffix               :: string(),% 复制数据文件的后缀，字符串类型，用于标识复制数据文件。
                                 file_repl_data                      :: string(),% 复制数据文件的完整名称，字符串类型，指定复制数据文件的具体名称。
                                 file_repl_log                       :: standard_io | string(),% 复制日志文件，可以是标准输出（standard_io）或指定的文件路径（字符串类型）。
                                 debug_mode=false                    :: true | false,% 调试模式开关，布尔类型，true 表示开启调试模式，false 表示关闭。默认值为 false。
                                 sup_restart_intensity               :: non_neg_integer(),% 监控树重启强度，非负整数，【表示在指定时间内允许的最大重启次数】。【配置监督器的重启策略】
                                 sup_restart_period                  :: non_neg_integer(),% 监控树重启周期，非负整数，与 sup_restart_intensity 配合使用，指定重启次数统计的时间范围。
                                 sup_child_shutdown                  :: non_neg_integer()}).% 监控树子进程关闭超时时间，非负整数，单位为毫秒，表示子进程关闭操作的最大等待时间。

% 定义存储键的记录结构，用于唯一标识存储中的数据项。
-record(eh_storage_key,         {object_type                         :: atom(),% 数据对象的类型，通常为原子类型，用于对数据进行分类。
                                 object_id                           :: term()}).% 数据对象的唯一标识符，类型可变，用于唯一标识一个数据对象。
% 定义存储值的记录结构，包含数据的时间戳、索引、状态、列名和具体值。
-record(eh_storage_value,       {timestamp                           :: non_neg_integer(),% 数据写入时的时间戳，非负整数，用于记录数据的版本信息。
                                 data_index                          :: non_neg_integer(),% 数据的索引，非负整数，用于在存储中定位数据。
                                 status=?STATUS_ACTIVE               :: ?STATUS_ACTIVE | ?STATUS_INACTIVE,% 数据的状态，?STATUS_ACTIVE 表示数据有效，?STATUS_INACTIVE 表示数据无效。默认值为 ?STATUS_ACTIVE。
                                 column                              :: atom(),% 数据所属的列名，通常为原子类型，用于标识数据的属性。
                                 value                               :: term()}).% 数据的具体值，类型可变，存储实际的数据内容。
% 定义存储数据的记录结构，整合了存储键和存储值的信息。
-record(eh_storage_data,        {object_type                         :: atom(),% 数据对象的类型，通常为原子类型，用于对数据进行分类。
                                 object_id                           :: term(),% 数据对象的唯一标识符，类型可变，用于唯一标识一个数据对象。
                                 timestamp                           :: non_neg_integer(),% 数据写入时的时间戳，非负整数，用于记录数据的版本信息。
                                 data_index                          :: non_neg_integer(),% 数据的索引，非负整数，用于在存储中定位数据。
                                 status=?STATUS_ACTIVE               :: ?STATUS_ACTIVE | ?STATUS_INACTIVE,% 数据的状态，?STATUS_ACTIVE 表示数据有效，?STATUS_INACTIVE 表示数据无效。默认值为 ?STATUS_ACTIVE。
                                 column                              :: atom(),% 数据所属的列名，通常为原子类型，用于标识数据的属性。
                                 value                               :: term()}).% 数据的具体值，类型可变，存储实际的数据内容。
% 定义更新消息键的记录结构，用于标识更新消息。
-record(eh_update_msg_key,      {timestamp                           :: non_neg_integer(),% 更新消息的时间戳，非负整数，用于记录消息的生成时间。
                                 object_type                         :: atom(),% 要更新的数据对象的类型，通常为原子类型，用于对数据进行分类。
                                 object_id                           :: term()}).% 要更新的数据对象的唯一标识符，类型可变，用于唯一标识一个数据对象。
% 定义更新消息数据的记录结构，包含更新数据、客户端信息、节点信息和消息引用。
-record(eh_update_msg_data,     {update_data                         :: term(),% 要更新的数据内容，类型可变，存储实际的更新数据。
                                 client_id                           :: pid(),% 发起更新请求的客户端进程 ID，用于跟踪请求来源。
                                 node_id                             :: atom(),% 处理更新请求的节点 ID，通常为原子类型，用于标识处理节点。
                                 msg_ref                             :: term()}).% 消息的引用，类型可变，用于唯一标识一条更新消息。
% 链节点存储的信息，用于存储链节点的各种状态信息和运行时数据。
-record(eh_system_state,        {node_status=?EH_NOT_READY           :: ?EH_NOT_READY | 
                                                                        ?EH_TRANSIENT |
                                                                        ?EH_TRANSIENT_TU |
                                                                        ?EH_TRANSIENT_DU |
                                                                        ?EH_READY,
                                % 节点的当前状态，初始值为 ?EH_NOT_READY。
                                % 可能的状态包括：
                                % - ?EH_NOT_READY: 节点未就绪，无法处理请求。
                                % - ?EH_TRANSIENT: 节点处于过渡状态。
                                % - ?EH_TRANSIENT_TU: 节点时间戳已更新的过渡状态。
                                % - ?EH_TRANSIENT_DU: 节点数据已更新的过渡状态。
                                % - ?EH_READY: 节点就绪，可以处理请求。
                                 timestamp=0                         :: non_neg_integer(),% 节点的时间戳，用于记录操作的时间顺序，初始值为 0。
                                 repl_ring_order=[]                  :: list(), % 列表，记录了节点在环中的排列顺序，方便系统按顺序处理节点相关操作。
                                 repl_ring=[]                        :: list(), % 列表，存储复制环中节点的信息,每个元素通常代表一个节点。借助 repl_ring，系统能知晓参与复制的节点有哪些，进而进行数据同步、故障转移等操作。
                                 predecessor                         :: atom(),% 节点在复制环中的前驱节点，用原子类型表示节点名称。
                                 successor                           :: atom(),% 节点在复制环中的后继节点，用原子类型表示节点名称。
                                 pre_msg_data=maps:new()             :: maps:map(),% 预消息数据映射，用于存储预消息处理阶段的相关数据。
                                 msg_data=maps:new()                 :: maps:map(),% 已处理消息数据映射，存储已经处理完成的消息相关数据。
                                 completed_set=sets:new()            :: sets:set(),% 已完成消息的集合，用于记录已经成功处理的消息，避免重复处理。
                                 pending_pre_msg_data=maps:new()     :: maps:map(),% 待处理预消息数据映射，存储等待处理的预消息相关数据。
                                 query_data=maps:new()               :: maps:map(),% 查询数据映射，存储节点处理查询请求时产生的相关数据。
                                 snapshot_ref                        :: term(),% 快照引用，用于引用节点的快照信息，类型可变。
                                 app_config                          :: #eh_app_config{}}).% 应用配置记录，包含节点、故障检测、数据管理等相关配置信息。
% 定义数据状态记录，用于存储节点的数据相关状态信息和运行时数据。
-record(eh_data_state,          {timestamp=0                         :: non_neg_integer(),% 数据的时间戳，用于记录数据操作的时间顺序，初始值为 0。
                                 transient_timestamp=0               :: non_neg_integer(),% 临时数据的时间戳，用于记录临时数据操作的时间顺序，初始值为 0。
                                 data_index_list=[]                  :: list(), % 数据索引列表，用于存储数据的索引信息，方便快速定位和访问数据。
                                 data=maps:new()                     :: maps:map(),%数据映射，存储节点的正式数据信息。
                                 transient_data=queue:new()          :: queue:queue(),% 临时数据队列，存储节点的临时数据信息，使用队列结构保证数据处理顺序。
				 data_update_count=0                 :: non_neg_integer(),% 数据更新次数，记录节点数据的更新操作次数，初始值为 0。
				 file_version_num=0                  :: non_neg_integer(),% 文件版本号，记录数据文件的版本信息，初始值为 0。
                                 file                                :: file:io_device(),% 文件设备，用于与数据文件进行交互的文件操作设备。
                                 app_config                          :: #eh_app_config{}}).% 应用配置记录，包含节点、故障检测、数据管理等相关配置信息。



