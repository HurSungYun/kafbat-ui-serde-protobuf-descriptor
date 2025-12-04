.PHONY: help build test clean jar dev quick verify format format-check proto-compile integration-test integration-topics integration-test-message integration-stop integration-clean s3-test s3-test-start s3-topic-mapping-test s3-topic-mapping-start s3-test-stop s3-test-clean s3-test-logs rustfs-logs integration-test-full check-mc install-mc
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

check-mc: ## Check if mc (MinIO Client) is installed
	@command -v mc >/dev/null 2>&1 || { echo "⚠️  MinIO Client (mc) not found. Installing..."; $(MAKE) install-mc; }

install-mc: ## Install MinIO Client (mc)
	@echo "📥 Installing MinIO Client (mc)..."
	@if [ "$$(uname)" = "Darwin" ]; then \
		if command -v brew >/dev/null 2>&1; then \
			brew install minio/stable/mc || { \
				echo "⚠️  Homebrew installation failed, trying direct download..."; \
				wget https://dl.min.io/client/mc/release/darwin-amd64/mc -O /tmp/mc && \
				chmod +x /tmp/mc && \
				sudo mv /tmp/mc /usr/local/bin/; \
			}; \
		else \
			wget https://dl.min.io/client/mc/release/darwin-amd64/mc -O /tmp/mc && \
			chmod +x /tmp/mc && \
			sudo mv /tmp/mc /usr/local/bin/; \
		fi; \
	elif [ "$$(uname)" = "Linux" ]; then \
		wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc && \
		chmod +x /tmp/mc && \
		sudo mv /tmp/mc /usr/local/bin/; \
	else \
		echo "❌ Unsupported OS. Please install MinIO Client (mc) manually from https://min.io/docs/minio/linux/reference/minio-mc.html"; \
		exit 1; \
	fi
	@echo "✅ MinIO Client (mc) installed successfully"

build: ## Build the project
	./gradlew build

test: ## Run tests
	./gradlew test

clean: ## Clean build artifacts
	./gradlew clean

jar: ## Build shadow jar
	./gradlew shadowJar

dev: ## Development build (compile + test)
	./gradlew compileJava compileTestJava test

quick: ## Quick build (compile only, skip tests)
	./gradlew assemble

verify: ## Full verification (clean + build + check + format)
	./gradlew clean spotlessCheck build check

format: ## Apply code formatting
	./gradlew spotlessApply

format-check: ## Check code formatting
	./gradlew spotlessCheck

proto-compile: ## Compile proto files and generate descriptor set
	protoc --descriptor_set_out=src/test/resources/test_descriptors.desc \
		--include_imports \
		--proto_path=src/test/proto \
		src/test/proto/user.proto \
		src/test/proto/order.proto \
		src/test/proto/nested.proto \
		src/test/proto/wkt.proto \
		src/test/proto/validator_test.proto

# Integration testing
integration-test: ## Start integration test environment
	cd docker-compose && ./start-integration-test.sh

integration-topics: ## Create test topics only
	cd docker-compose && docker-compose --profile setup run --rm topic-creator

integration-test-message: ## Send a test protobuf message
	cd docker-compose && ./scripts/send_test_message.sh

integration-stop: ## Stop integration test environment
	cd docker-compose && docker-compose down

integration-clean: ## Clean integration test environment
	cd docker-compose && docker-compose down -v --remove-orphans

# S3 Integration testing
s3-test: check-mc ## Start S3 integration test with RustFS
	cd docker-compose && ./test-s3-integration.sh

s3-test-start: check-mc ## Start S3 test environment (interactive)
	cd docker-compose && ./start-s3-integration-test.sh

s3-topic-mapping-test: check-mc ## Start S3 topic mapping integration test
	cd docker-compose && ./test-s3-topic-mapping-integration.sh

s3-topic-mapping-start: check-mc ## Start S3 topic mapping test environment (interactive)
	cd docker-compose && ./start-s3-topic-mapping-test.sh

s3-test-stop: ## Stop S3 test environment
	cd docker-compose && docker-compose --profile s3-test down

s3-test-clean: ## Clean S3 test environment completely
	cd docker-compose && docker-compose --profile s3-test down -v --remove-orphans

s3-test-logs: ## Show S3 test logs
	cd docker-compose && docker-compose --profile s3-test logs -f kafka-ui-s3

rustfs-logs: ## Show RustFS logs
	cd docker-compose && docker-compose logs -f rustfs

integration-test-full: ## Run both local and S3 integration tests
	$(MAKE) integration-test
	$(MAKE) s3-test
