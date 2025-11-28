// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package settings

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	// "encoding/json"  // 用于 JSON 编码和解码
	// "os"            // 用于操作系统功能
	// "path/filepath" // 用于文件路径操作
	// "reflect"       // 用于反射操作
)

// getParsedConfig 解析配置文件并返回键值对映射
func getParsedConfig(fn string) map[string]interface{} {
	// 检查文件是否存在，如果不存在则返回 nil
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return nil
	}
	// 创建一个空的映射来存储解析后的配置
	m := map[string]interface{}{}
	// 读取文件内容
	b, err := os.ReadFile(filepath.Clean(fn))
	if err != nil {
		// 如果读取失败则抛出异常
		panic(err)
	}
	// 将 JSON 数据解析到映射中
	if err := json.Unmarshal(b, &m); err != nil {
		// 如果解析失败则抛出异常
		panic(err)
	}
	// 返回解析后的配置映射
	return m
}

// overwriteHardSettings 覆盖硬设置配置
func overwriteHardSettings(org *hard) {
	// 获取解析后的硬设置配置文件
	cfg := getParsedConfig("dragonboat-hard-settings.json")
	// 使用反射获取硬设置结构体的间接值
	rd := reflect.Indirect(reflect.ValueOf(org))
	// 调用通用设置覆盖函数
	overwriteSettings(cfg, rd)
}

// overwriteSoftSettings 覆盖软设置配置
func overwriteSoftSettings(org *soft) {
	// 获取解析后的软设置配置文件
	cfg := getParsedConfig("dragonboat-soft-settings.json")
	// 使用反射获取软设置结构体的间接值
	rd := reflect.Indirect(reflect.ValueOf(org))
	// 调用通用设置覆盖函数
	overwriteSettings(cfg, rd)
}

// overwriteSettings 通用设置覆盖函数
func overwriteSettings(cfg map[string]interface{}, rd reflect.Value) {
	// 遍历配置中的每个键值对
	for key, val := range cfg {
		// 通过字段名获取结构体字段
		field := rd.FieldByName(key)
		// 检查字段是否有效（存在）
		if field.IsValid() {
			// 根据字段类型进行相应的处理
			switch field.Type().String() {
			case "uint64":
				// 将接口值转换为 uint64（JSON 中数字默认为 float64）
				nv := uint64(val.(float64))
				// 记录设置更改日志
				plog.Infof("Setting %s to uint64 value %d", key, nv)
				// 设置字段值
				field.SetUint(nv)
			case "bool":
				// 记录设置更改日志
				plog.Infof("Setting %s to bool value %t", key, val.(bool))
				// 设置布尔字段值
				field.SetBool(val.(bool))
			case "string":
				// 记录设置更改日志
				plog.Infof("Setting %s to string value %s", key, val.(string))
				// 设置字符串字段值
				field.SetString(val.(string))
			default:
				// 对于不支持的类型不进行处理
			}
		}
	}
}
