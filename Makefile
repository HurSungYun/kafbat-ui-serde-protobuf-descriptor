.PHONY: help build test clean check jar install wrapper
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the project
	./gradlew build

test: ## Run tests
	./gradlew test

clean: ## Clean build artifacts
	./gradlew clean

check: ## Run code quality checks and tests
	./gradlew check

jar: ## Build shadow jar
	./gradlew shadowJar

install: ## Install to local repository
	./gradlew publishToMavenLocal

wrapper: ## Update Gradle wrapper
	gradle wrapper

dev: ## Development build (compile + test)
	./gradlew compileJava compileTestJava test

quick: ## Quick build (compile only, skip tests)
	./gradlew assemble

verify: ## Full verification (clean + build + check)
	./gradlew clean build check

deps: ## Show project dependencies
	./gradlew dependencies

tasks: ## Show available Gradle tasks
	./gradlew tasks

init: ## Initialize project (setup wrapper)
	gradle wrapper
	chmod +x gradlew

proto: ## Generate protobuf descriptor sets
	./gradlew generateTestDescriptors

proto-compile: ## Compile proto files manually
	protoc --descriptor_set_out=src/test/resources/test_descriptors.desc \
		--include_imports \
		--proto_path=src/test/proto \
		src/test/proto/user.proto \
		src/test/proto/order.proto

# Integration testing
integration-test: ## Start integration test environment
	cd docker && ./start-integration-test.sh

integration-test-full: ## Start integration test with producer
	cd docker && ./start-integration-test.sh --with-producer

integration-topics: ## Create test topics only
	cd docker && docker-compose --profile setup run --rm topic-creator

integration-test-message: ## Send a test protobuf message
	cd docker && docker run --rm --network docker_default -v "$$(pwd)/descriptors:/descriptors" -v "$$(pwd)/scripts:/scripts" python:3.11-slim bash -c "pip install protobuf==4.24.4 > /dev/null 2>&1 && cd /scripts && python test_protobuf_message.py 2>/dev/null" | docker exec -i kafka-protobuf-test kafka-console-producer --bootstrap-server kafka:29092 --topic user-events 2>/dev/null && echo "✅ Test message sent!"

integration-stop: ## Stop integration test environment
	cd docker && docker-compose down

integration-clean: ## Clean integration test environment
	cd docker && docker-compose down -v --remove-orphans