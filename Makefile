BUILD_DIR ?= build

.PHONY: dev release test bench netbench bench-all crash-suite fmt fmt-check asan docker-build docker-run docker-smoke

dev:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD_DIR) -j
	./$(BUILD_DIR)/pomai_cache_server --port 6379 --policy pomai_cost --params config/policy_params.json

release:
	cmake -S . -B $(BUILD_DIR)-release -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR)-release -j

asan:
	cmake -S . -B $(BUILD_DIR)-asan -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS='-fsanitize=address,undefined -fno-omit-frame-pointer' -DCMAKE_EXE_LINKER_FLAGS='-fsanitize=address,undefined'
	cmake --build $(BUILD_DIR)-asan -j
	ctest --test-dir $(BUILD_DIR)-asan --output-on-failure

test:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD_DIR) -j
	ctest --test-dir $(BUILD_DIR) --output-on-failure

bench:
	cmake -S . -B $(BUILD_DIR)-release -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR)-release -j
	./$(BUILD_DIR)-release/pomai_cache_bench

netbench:
	cmake -S . -B $(BUILD_DIR)-release -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR)-release -j
	./$(BUILD_DIR)-release/pomai_cache_netbench

bench-all:
	./scripts/bench_run.sh

fmt:
	find include src apps bench tests -name '*.hpp' -o -name '*.cpp' | xargs clang-format -i

fmt-check:
	find include src apps bench tests -name '*.hpp' -o -name '*.cpp' | xargs clang-format --dry-run --Werror

docker-build:
	docker build -f docker/Dockerfile -t pomai-cache:latest .

docker-run:
	docker compose -f docker/docker-compose.yml up --build

docker-smoke: docker-build
	docker run --rm -d --name pomai-cache-smoke -p 6390:6379 pomai-cache:latest
	sleep 2
	printf '*1\r\n$4\r\nPING\r\n' | nc 127.0.0.1 6390 | head -n 1
	docker rm -f pomai-cache-smoke

crash-suite:
	cmake -S . -B $(BUILD_DIR)-debug -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD_DIR)-debug -j
	cd $(BUILD_DIR)-debug && ./pomai_cache_crash_harness everysec
