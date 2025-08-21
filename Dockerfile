# 使用官方的 Go 基础镜像
FROM golang:1.24

# 设置工作目录
WORKDIR /app

# 复制项目文件到工作目录
# COPY . .
# 复制 go.mod 和 go.sum 文件
COPY go.mod go.sum ./

# 测试网络连接
# RUN apt-get update && apt-get install -y curl && curl -v https://proxy.golang.org
# 设置 Go 模块代理
ENV GOPROXY=https://mirrors.aliyun.com/goproxy/,direct
# 下载依赖
RUN go mod download

# 复制项目文件到工作目录
COPY . .
# 安装项目依赖
RUN go mod tidy

# 安装静态检查工具
RUN make install-static-check-tools