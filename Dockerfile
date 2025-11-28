#先在终端输入：go mod tidy
# 使用官方的 Go 基础镜像
FROM golang:1.24

# 设置工作目录
WORKDIR /app

# 复制 go.mod 和 go.sum 文件
COPY go.mod go.sum ./

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

# 此时可以顺利构建 Docker 镜像。
# 在（docker？）终端输入：docker build -t dragonboat-test .