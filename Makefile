BUILD_DIR ?= build

.PHONY: dev release test bench fmt docker-build docker-run

dev:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD_DIR) -j
	./$(BUILD_DIR)/pomai_cache_server --port 6379 --policy pomai_cost --params config/policy_params.json

release:
	cmake -S . -B $(BUILD_DIR)-release -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR)-release -j

test:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD_DIR) -j
	ctest --test-dir $(BUILD_DIR) --output-on-failure

bench:
	cmake -S . -B $(BUILD_DIR)-release -DCMAKE_BUILD_TYPE=Release
	cmake --build $(BUILD_DIR)-release -j
	./$(BUILD_DIR)-release/pomai_cache_bench

fmt:
	find include src apps bench tests -name '*.hpp' -o -name '*.cpp' | xargs clang-format -i

docker-build:
	docker build -f docker/Dockerfile -t pomai-cache:latest .

docker-run:
	docker compose -f docker/docker-compose.yml up --build
