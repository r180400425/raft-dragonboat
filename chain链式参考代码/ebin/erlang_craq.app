%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copyright (c) 2016 Gyanendra Aggarwal.  All Rights Reserved.
%% 
%%     erlang_craq.app：Erlang 应用的描述文件，包含应用的元数据，如应用名称、版本、依赖的应用等，用于 Erlang 运行时系统识别和管理应用。
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

{application, erlang_craq,
 [ {description, "chain replication apportioned query high throughput atomic store"}
  ,{vsn, "0.1.0"}
  ,{modules, [eh_app, eh_sup, eh_system_sup, eh_system_server, eh_event, eh_data_server]}
  ,{registered, [eh_sup, eh_system_sup, eh_system_server, eh_event, eh_data_server]}
  ,{applications, [kernel, stdlib]}
  ,{mod, {eh_app, []}}
 ]}.