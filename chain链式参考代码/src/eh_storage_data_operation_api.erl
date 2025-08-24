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


% 技术特点
  % 使用 -spec 进行类型规范，增强代码可读性和安全性。
  % 利用 queue 实现异步写入。
  % 支持版本控制和检查点机制，用于数据一致性管理。
  % 通过 EntryOperation 解耦具体数据格式，提高扩展性。

% 模块协作
% eh_storage_data_operation_api 作为数据操作接口层，依赖：
%   eh_persist_storage_data：底层文件操作（打开、读写、关闭）。
%   eh_system_config：获取存储配置。
%   eh_data_util：处理时间戳、索引和数据结构。
%   EntryOperation：数据序列化与反序列化（如 eh_storage_data_operation）。
% 该模块是数据持久化操作的核心接口，负责将数据从文件中读取、解析、写入，支持版本控制和高效更新，是构建数据服务的基础组件。

% 提供数据持久化操作的 API 接口。
-module(eh_storage_data_operation_api).

-export([open/1,
         close/1,
         read/2,
	 read_all/2,
         write/5]).

-include("erlang_craq.hrl").

% -spec：声明函数的类型规范。
% 调用底层模块 eh_persist_storage_data 打开指定文件。
-spec open(FileName :: string()) -> {ok, file:io_device()} | {error, atom()}.
open(FileName) ->
  eh_persist_storage_data:open_data_file(FileName).%这是实际的实现函数

% 关闭文件句柄
-spec close(File :: file:io_device()) -> ok | {error, atom()}.
close(File) ->
  eh_persist_storage_data:close_data_file(File).

% 根据配置获取数据操作模块，递归读取多个文件内容。
-spec read_all(AppConfig :: #eh_app_config{}, FileNames :: list()) -> {ok | error, non_neg_integer(), list(), maps:map()}.
read_all(AppConfig, FileNames) ->
    EntryOperation = eh_system_config:get_storage_data(AppConfig),
    read_all(EntryOperation, FileNames, {ok, 0, [], maps:new()}).

% 依次打开文件、读取内容、关闭文件，累积读取结果。
-spec read_all(EntryOperation :: atom(), FileNames :: list(), Acc :: term()) -> {ok | error, non_neg_integer(), list(), maps:map()}.
read_all(EntryOperation, [FileName | RFileNames], {_, Timestamp, DataIndex, M0}) ->
    {ok, File} = open(FileName),
    Acc = read(EntryOperation, File, 0, {Timestamp, DataIndex}, M0),%ACC累加器，M0是maps 用于存储当前数据
    close(File),
    read_all(EntryOperation, RFileNames, Acc);
read_all(_EntryOperation, [], Acc) ->
    Acc.

% 根据配置获取数据操作模块，并调用 read/6 读取整个文件。
-spec read(AppConfig :: #eh_app_config{}, File :: file:io_device()) ->  {ok | error, non_neg_integer(), list(), maps:map()}.
read(AppConfig, File) ->
  EntryOperation = eh_system_config:get_storage_data(AppConfig),
  read(EntryOperation, File, 0, {0, []}, maps:new()).%初始化读取位置为 0，时间戳为 0，数据索引为空列表，数据为空 map。



%% @spec read(EntryOperation :: atom(), File :: file:io_device(), Loc :: non_neg_integer(), {Timestamp :: non_neg_integer(), DataIndex :: list()}, M0 :: maps:map()) 
%%      -> {ok | error, non_neg_integer(), list(), maps:map()}.
%% @doc 从指定文件的特定位置开始递归读取数据。
%%      该函数会读取数据头和数据体，解析为结构化数据，并更新时间戳和数据索引。
%% @param EntryOperation 数据操作模块名，原子类型，用于处理数据的序列化和反序列化。
%% @param File 文件句柄，用于操作文件。
%% @param Loc 当前读取的文件偏移量，非负整数。
%% @param {Timestamp, DataIndex} 时间戳和数据索引的元组，用于记录数据版本和索引信息。
%% @param M0 存储数据的映射，初始状态或上一次读取的结果。
%% @return 包含读取状态（ok 或 error）、时间戳、数据索引和数据映射的元组。
%% @end
-spec read(EntryOperation :: atom(), File :: file:io_device(), Loc :: non_neg_integer(), {Timestamp :: non_neg_integer(), DataIndex :: list()}, M0 :: maps:map()) 
      -> {ok | error, non_neg_integer(), list(), maps:map()}.
read(EntryOperation, File, Loc, {Timestamp, DataIndex}, M0) ->
  % 从文件的指定位置读取数据头，读取长度由 EntryOperation 模块定义的头字节大小决定
  case eh_persist_storage_data:read_data(File, Loc, EntryOperation:header_byte_size()) of
    % 若到达文件末尾，返回当前的时间戳、数据索引和数据映射
    eof               -> 
      {ok, Timestamp, DataIndex, M0};
    % 若读取数据头出错，返回错误信息及当前的时间戳、数据索引和数据映射
    {error, _}        -> 
      {error, Timestamp, DataIndex, M0};
    % 若成功读取数据头，获取下一个读取位置和数据头内容
    {ok, Loc1, HData} ->
      % 从数据头中获取数据体的大小
      DataSize =  EntryOperation:entry_header(HData),
      % 根据数据体大小，从新的位置继续读取数据体
      case eh_persist_storage_data:read_data(File, Loc1, DataSize) of
        % 若读取数据体时到达文件末尾，截断文件到当前读取位置，并返回错误信息
        eof               ->
          eh_persist_storage_data:truncate_data(File, Loc),
          {error, Timestamp, DataIndex, M0};
        % 若读取数据体出错，返回错误信息及当前的时间戳、数据索引和数据映射
        {error, _}        ->
          {error, Timestamp, DataIndex, M0};
        % 若成功读取数据体，获取下一个读取位置和数据体内容
        {ok, Loc2, RData} ->
          % 将数据头和数据体转换为结构化条目
          case EntryOperation:binary_to_entry(HData, RData) of
            % 若转换失败，截断文件到当前读取位置，并返回错误信息
            ?EH_BAD_DATA ->
              eh_persist_storage_data:truncate_data(File, Loc),
              {error, Timestamp, DataIndex, M0};
            % 若转换成功，更新时间戳和数据索引，添加键值对到数据映射中，并继续递归读取
            {ok, Entry}  ->
              read(EntryOperation, File, Loc2, eh_data_util:update_timestamp(Entry, {Timestamp, DataIndex}), eh_data_util:add_key_value(Entry, M0))
          end
      end
  end.





%% @spec write(AppConfig :: #eh_app_config{}, File :: file:io_device(), Q0 :: queue:queue(), FileVersionNum :: non_neg_integer(), DataUpdateCount :: non_neg_integer()) 
%%      -> {ok, file:io_device(), non_neg_integer(), non_neg_integer()} | {error, atom()}.
%% @doc 将队列中的数据写入文件，并根据数据更新次数决定是否创建新版本文件。
%%      当数据更新次数达到检查点阈值时，会读取当前文件内容，若读取成功则关闭当前文件，创建新版本文件。
%% @param AppConfig 应用配置记录，包含系统的各项配置信息。
%% @param File 文件句柄，用于操作要写入数据的文件。
%% @param Q0 待写入数据的队列，队列中的元素为要写入文件的数据条目。
%% @param FileVersionNum 当前文件的版本号，非负整数。
%% @param DataUpdateCount 数据更新次数，非负整数，用于判断是否达到检查点。
%% @return 若操作成功，返回包含新文件句柄、新文件版本号和重置后数据更新次数的元组；若出错，返回错误信息。
%% @end
-spec write(AppConfig :: #eh_app_config{}, File :: file:io_device(), Q0 :: queue:queue(), FileVersionNum :: non_neg_integer(), DataUpdateCount :: non_neg_integer()) 
            -> {ok, file:io_device(), non_neg_integer(), non_neg_integer()} | {error, atom()}.          
write(AppConfig, File, Q0, FileVersionNum, DataUpdateCount) ->
  % 从应用配置中获取数据操作模块名
  EntryOperation = eh_system_config:get_storage_data(AppConfig),
  % 递归将队列中的数据条目写入文件
  write_entries(EntryOperation, File, Q0),
  % 从应用配置中获取数据检查点阈值
  DataCheckPoint = eh_system_config:get_data_checkpoint(AppConfig),
  % 判断数据更新次数是否达到检查点阈值
  case DataUpdateCount >= DataCheckPoint of
    % 若达到检查点阈值
    true  ->
      % 读取当前文件的内容
      case read(AppConfig, File) of
        % 若读取成功
        {ok, _, _, _}    ->
          % 关闭当前文件
          close(File),
          % 生成新的文件版本号
          FileVersionNum1 = FileVersionNum + 1,
          % 根据新的文件版本号和应用配置生成带版本号的完整文件名
          VersionedFileName = eh_file_name:get_full_versioned_file_name(FileVersionNum1, AppConfig),
          % 打开新的版本文件
          {ok, File1} = open(VersionedFileName),
          % 返回新文件句柄、新文件版本号和重置后的数据更新次数
          {ok, File1, FileVersionNum1, 0};
        % 若读取文件出错
        {error, _, _, _} ->
          % 返回错误信息，包含当前文件句柄、当前文件版本号和更新后的数据更新次数
          {error, File, FileVersionNum, DataUpdateCount + 1}
      end;
    % 若未达到检查点阈值
    false ->
      % 返回当前文件句柄、当前文件版本号和更新后的数据更新次数
      {ok, File, FileVersionNum, DataUpdateCount + 1}
  end.


% 递归写入队列中的每一条数据。
-spec write_entries(EntryOperation :: atom(), File :: file:io_device(), Q0 :: queue:queue()) -> ok | {error, atom()}.
write_entries(EntryOperation, File, Q0) ->
  case queue:out(Q0) of
    {empty, _}           ->
      file:sync(File);
    {{value, Entry}, Q1} ->
      ok = write_entry(EntryOperation, File, Entry),
      write_entries(EntryOperation, File, Q1)
  end.

% 写入单条数据
% 将结构化条目转换为二进制，并写入文件。
-spec write_entry(EntryOperation :: atom(), File :: file:io_device(), Entry :: #eh_storage_data{}) -> ok | {error, atom()}.                      
write_entry(EntryOperation, File, Entry) ->
  Bin = EntryOperation:entry_to_binary(Entry),
  eh_persist_storage_data:write_data(File, Bin).




    
  
