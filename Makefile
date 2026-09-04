tag ?= $$(git rev-parse --short HEAD)
GO        ?= go
SERVER    := ./example
BINARY    := broker-server
BIN_DIR   := bin

# 交叉编译工具链(可用 make CROSS_SDK=... 覆盖)
CROSS_SDK := /opt/sdk/gcc-linaro-6.3.1-2017.05-x86_64_aarch64-linux-gnu
CROSS_CC  := $(CROSS_SDK)/bin/aarch64-linux-gnu-gcc

.PHONY: build run vet test cross clean

# 本机构建
build:
	$(GO) build -o $(BIN_DIR)/$(BINARY) $(SERVER)

# 本地运行
run:
	$(GO) run $(SERVER)

vet:
	$(GO) vet ./...

test:
	$(GO) test -count=1 ./...

# 交叉构建示例 server(linux/arm64,CGO 走 linaro 工具链,-N -l 保留调试信息)
cross:
	mkdir -p $(BIN_DIR)                                            \
	&& export LIBRARY_PATH=$$LIBRARY_PATH:$(CROSS_SDK)/lib/        \
	&& export LD_LIBRARY_PATH=$$LD_LIBRARY_PATH:$(CROSS_SDK)/lib/  \
	&& export CC=$(CROSS_CC)                                       \
	&& CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build -gcflags "-N -l" -o $(BIN_DIR)/$(BINARY)-linux-arm64 $(SERVER)

clean:
	rm -rf $(BIN_DIR)


image:
	export HTTP_PROXY=http://192.168.66.170:42059																		\
		&& export HTTPS_PROXY=http://192.168.66.170:42059																\
		&& podman build --network host -t registry.cn-hangzhou.aliyuncs.com/etsme/etslms-broker:${tag} .
image-push:
	podman push registry.cn-hangzhou.aliyuncs.com/etsme/local-sms:${tag}
