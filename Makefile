CC = clang
CFLAGS_WASM = --target=wasm32-unknown-unknown -O3 -nostdlib -fno-builtin -Iinclude -Wall -Wextra
LDFLAGS_WASM = -Wl,--no-entry -Wl,--export-dynamic -Wl,--allow-undefined -Wl,-z,stack-size=1048576

CC_NATIVE = clang
CFLAGS_NATIVE = -O2 -Iinclude -Wall -Wextra

SRC_DIR = src
INC_DIR = include
TEST_DIR = test
BUILD_DIR = build

CFLAGS_WASM64 = --target=wasm64-unknown-unknown -O3 -nostdlib -fno-builtin -Iinclude -Wall -Wextra -msimd128 -matomics -mbulk-memory -mmutable-globals -mreference-types
LDFLAGS_WASM64 = -Wl,--no-entry -Wl,--export-dynamic -Wl,--allow-undefined -Wl,-mwasm64 -Wl,-z,stack-size=1048576 -Wl,--max-memory=17179869184

WASM64_TARGET = yowza64.wasm

WASM_TARGET = yowza.wasm
TEST_TARGET = test_runner
DAEMON_TARGET = yowzad

SRCS = $(wildcard $(SRC_DIR)/*.c)
# Exclude yowzad.c from the Wasm build since it has POSIX networking and stdio
WASM_SRCS = $(filter-out $(SRC_DIR)/yowzad.c, $(SRCS))

# Exclude yowza.c (Wasm exports) and yowzad.c (daemon main) from tests
TEST_SRCS = $(wildcard $(TEST_DIR)/*.c) $(filter-out $(SRC_DIR)/yowza.c $(SRC_DIR)/yowzad.c, $(SRCS))

# Daemon sources: need the C native engine but omit Wasm exports
DAEMON_SRCS = $(filter-out $(SRC_DIR)/yowza.c $(SRC_DIR)/storage_memory.c, $(SRCS))

all: $(WASM_TARGET) $(WASM64_TARGET) $(DAEMON_TARGET)

$(WASM64_TARGET): $(WASM_SRCS)
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS_WASM64) $(LDFLAGS_WASM64) $(WASM_SRCS) -o $(BUILD_DIR)/$(WASM64_TARGET)
	@cp $(BUILD_DIR)/$(WASM64_TARGET) .

$(WASM_TARGET): $(WASM_SRCS)
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS_WASM) $(LDFLAGS_WASM) $(WASM_SRCS) -o $(BUILD_DIR)/$(WASM_TARGET)
	@cp $(BUILD_DIR)/$(WASM_TARGET) .

$(DAEMON_TARGET): $(DAEMON_SRCS)
	$(CC_NATIVE) $(CFLAGS_NATIVE) $(DAEMON_SRCS) -o $(DAEMON_TARGET)

test: $(TEST_TARGET)
	./$(TEST_TARGET)

$(TEST_TARGET): $(TEST_SRCS)
	$(CC_NATIVE) $(CFLAGS_NATIVE) $(TEST_SRCS) -o $(TEST_TARGET)

clean:
	rm -rf $(BUILD_DIR) $(WASM_TARGET) $(WASM64_TARGET) $(TEST_TARGET) $(DAEMON_TARGET) yowzad_native.wal

.PHONY: all test clean
