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

% 具体实现

% 该模块实现了 eh_data_server 行为，提供了对数据的查询、更新和存储功能。
-module(eh_data_server).
% -behavior/1：声明该模块遵循 gen_server 行为，提供通用服务器功能。
-behavior(gen_server).

% 声明模块中需要导出的函数，以便其他模块可以调用。

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlang_craq.hrl").

% 作用：定义一个宏 SERVER，其值为 ?EH_DATA_SERVER，通常用于模块间通信。
-define(SERVER, ?EH_DATA_SERVER).

% 启动一个本地 gen_server 进程。
start_link(AppConfig) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [AppConfig], []).
% gen_server:start_link/4：启动服务器，绑定进程到本地名称 ?SERVER，并传递初始化参数 [AppConfig]。
% {local, ?SERVER}：表示服务器以本地名称注册。

% 初始化服务器状态，包括从文件中读取数据、打开文件并设置初始状态。
init([AppConfig]) ->
  DataDir = eh_system_config:get_data_dir(AppConfig), %获取配置中的数据目录
  FileName = eh_system_config:get_file_repl_data(AppConfig), %获取配置中的文件名
  {_, Timestamp, DataIndexList, D0} = eh_storage_data_operation_api:read_all(AppConfig, eh_file_name:get_full_versioned_file_names(DataDir, FileName)), %读取所有数据
  FileVersionNum = eh_file_name:get_version_num(DataDir, FileName)+1, %获取文件版本号
  VersionedFileName = eh_file_name:get_full_versioned_file_name(FileVersionNum, AppConfig), %获取带版本号的文件名
  {ok, File} = eh_storage_data_operation_api:open(VersionedFileName), %打开文件
  State = #eh_data_state{timestamp=Timestamp, 
			 data_index_list=DataIndexList, 
			 data=D0, 
			 file_version_num=FileVersionNum, 
			 file=File, 
			 app_config=AppConfig}, %初始化状态
  {ok, State}. %返回状态


% gen_server 同步调用机制
%   使用 gen_server:call(ServerName, Request) 发起请求。
%   服务器通过 handle_call(Request, From, State) 处理请求。
%   返回 {reply, Reply, NewState} 给调用者。



% 处理同步请求
% 处理获取时间戳和数据索引列表的请求。
% 返回 {timestamp, data_index_list}。
handle_call(?EH_TIMESTAMP,  %请求内容（这里是时间戳）（用于区分不同操作）
            _From, %发送请求的进程标识，通常忽略。
            State) -> %当前服务器状态
  {reply, {State#eh_data_state.timestamp, State#eh_data_state.data_index_list}, State};
% 从State记录中获取时间戳和数据索引列表。
% 返回值格式：{reply, Reply, NewState}
  % reply	固定原子，表示这是一个同步响应。
  % Reply	要返回给请求者的值，这里是 {Timestamp, DataIndexList}。
  % NewState	返回的新状态，这里未改变，直接返回原始 State。

% 查询（对象类型和对象ID）
% 返回 {ObjectType, ObjectId, Result}。
handle_call({?EH_QUERY, {ObjectType, ObjectId}}, 
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:query_data(ObjectType, ObjectId, Data),
  {reply, {ObjectType, ObjectId, Reply}, State};

% 根据时间戳和索引生成数据快照，用于一致性备份或恢复。
handle_call({?EH_SNAPSHOT, {Timestamp, DataIndex}}, 
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:snapshot_data(Timestamp, DataIndex, Data),
  {reply, Reply, State};

handle_call({?EH_UPDATE, {?EH_NOT_READY, Timestamp, UpdateList}}, 
            _From, 
            #eh_data_state{transient_timestamp=TTimestamp, transient_data=TData}=State) when Timestamp > TTimestamp ->
  TData1 = eh_data_util:make_transient_data(UpdateList, Timestamp, TData),
  {reply, ok, State#eh_data_state{transient_timestamp=Timestamp, transient_data=TData1}};

handle_call({?EH_UPDATE, {?EH_NOT_READY, _Timestamp, _}}, 
            _From, 
            State) ->
  {reply, ok, State};


%  功能说明：
%     接收到客户端发来的 ?EH_READY 状态的 ?EH_UPDATE 请求。
%     从状态中提取当前数据、临时数据、时间戳、索引列表。
%     调用 eh_data_util:make_data/5：
%     合并临时数据 TQ0 和当前数据 Data
%     构建新的数据 D0 和数据索引列表 DIL0
%     构建要写入的数据队列 Q0
%     调用 write_data/5：
%     将数据写入磁盘
%     更新服务器状态 NewState

% 将更新数据与现有数据合并，并写入新版本的文件。
handle_call({?EH_UPDATE, {?EH_READY, Timestamp, UpdateList}}, %参数1：请求内容，这里是 ?EH_UPDATE 类型，且状态为 ?EH_READY。
            _From, 
            #eh_data_state{data=Data, % 当前服务器的数据
			   transient_data=TQ0, % 临时数据队列（可能包括未提交的更新）
			   timestamp=StateTimestamp, 
			   data_index_list=StateDataIndexList}=State) ->
  {_, {_, DIL0}, Q0, D0} = eh_data_util:make_data(UpdateList, Timestamp, {StateTimestamp, StateDataIndexList}, TQ0, Data),
  write_data(Timestamp, DIL0, D0, Q0, State);

% 处理数据更新请求，将更新数据合并到现有数据中，并写入文件。
handle_call({?EH_UPDATE_SNAPSHOT, Qi0}, 
            _From, 
            #eh_data_state{data=Data, 
			   transient_data=TData, 
			   timestamp=StateTimestamp, 
			   data_index_list=StateDataIndexList}=State) ->
  {Timestamp, DIL0, Q0, D0} = eh_data_util:merge_data(Qi0, TData, {StateTimestamp, StateDataIndexList}, Data),
  write_data(Timestamp, DIL0, D0, Q0, State);

% 数据视图获取
handle_call(?EH_DATA_VIEW, 
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:data_view(Data),
  {reply, Reply, State};

handle_call({?EH_GET_DATA, {ObjectType, ObjectId}},
            _From,
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:get_data(ObjectType, ObjectId, Data),
  {reply, Reply, State};
        
% 数据校验 当前数据和给定数据
handle_call({?EH_CHECK_DATA, {Timestamp, DataList}},
            _From, 
            #eh_data_state{data=Data}=State) ->
  Reply = eh_data_util:check_data(Timestamp, DataList, Data),
  {reply, Reply, State}.

% 处理异步请求

% 处理异步请求，如停止服务器。
handle_cast({stop, Reason}, State) ->
  {stop, Reason, State}; %停止服务器并传递原因和状态。

handle_cast(_Msg, State) ->
  {noreply, State}.

% 处理非请求消息

% 处理非请求消息，如定时器或系统消息。
handle_info(_Msg, State) -> %_Msg：忽略消息。
  {noreply, State}.%不返回响应，保持状态不变。

% 代码热更新
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

% 终止函数
% 在服务器终止时关闭文件。
terminate(_Reason, #eh_data_state{file=File}) ->
  eh_storage_data_operation_api:close(File).

% 写入数据函数，并更新服务器状态
write_data(Timestamp, DIL0, D0, Q0, 
            #eh_data_state{file=File,
                           file_version_num=FileVersionNum,
                           data_update_count=DataUpdateCount,
                           app_config=AppConfig}=State) ->
    {Tag, File1, FileVersionNum1, DataUpdateCount1} = eh_storage_data_operation_api:write(AppConfig, File, Q0, FileVersionNum, DataUpdateCount),
    State1 = State#eh_data_state{timestamp=Timestamp, 
                                 file=File1,
	                         file_version_num=FileVersionNum1,
                                 data_update_count=DataUpdateCount1,
                                 data_index_list=DIL0,
                                 data=D0,
			         transient_data=queue:new()},
    case Tag of
	ok ->
	    {reply, ok, State1}; 
	_  ->
	    {stop, Tag, ok, State1}
    end.
      
