# Contributing to Protobuf Descriptor Set Serde

Thank you for your interest in contributing! This guide covers development setup, testing, and contribution guidelines.

## Development Setup

### Prerequisites

- **Java 17+**: Required for building the project
- **Protocol Buffers Compiler (protoc)**: Required for compiling .proto files
- **Docker & Docker Compose**: Required for integration testing
- **Git**: For version control

### Building from Source

1. **Clone the repository**:
   ```bash
   git clone https://github.com/hursungyun/kafka-ui-protobuf-descriptor-set-serde.git
   cd kafka-ui-protobuf-descriptor-set-serde
   ```

2. **Build the project**:
   ```bash
   make build
   # Or directly:
   ./gradlew build
   ```

3. **Run tests**:
   ```bash
   # Unit tests (fast)
   make test

   # Integration tests (requires Docker)
   make integration-test

   # Code formatting check
   make format-check

   # Apply code formatting
   make format
   ```

## Project Structure

```
├── src/main/java/io/github/hursungyun/kafbat/ui/serde/
│   ├── ProtobufDescriptorSetSerde.java          # Main serde implementation
│   ├── serialization/                            # Serialization/deserialization
│   ├── scheduler/                                # Background refresh scheduler
│   └── sources/                                  # Descriptor source abstractions
│       ├── DescriptorSource.java                 # Interface for all sources
│       ├── DescriptorSourceFactory.java          # Factory for creating sources
│       ├── LocalFileDescriptorSource.java        # Local file implementation
│       └── S3DescriptorSource.java              # S3/MinIO implementation
├── src/test/java/                               # Test files
├── src/test/proto/                              # Test protobuf definitions
│   ├── user.proto                               # User message definitions
│   ├── order.proto                              # Order message definitions
│   └── nested.proto                             # Nested message test cases
├── docker-compose/                              # Integration testing
│   ├── docker-compose.yml                       # Test environment
│   ├── start-integration-test.sh                # Local file integration test
│   ├── start-s3-integration-test.sh             # Interactive S3 test
│   └── test-s3-integration.sh                   # Automated S3 test
├── Makefile                                     # Build and test commands
└── build.gradle                                 # Build configuration
```

## Development Workflow

### Working with Protobuf Files

When modifying `.proto` files in `src/test/proto/`:

1. **Edit the proto file**:
   ```bash
   vim src/test/proto/nested.proto
   ```

2. **Regenerate descriptor set**:
   ```bash
   make proto-compile
   ```
   This compiles all proto files and generates `src/test/resources/test_descriptors.desc`

3. **Run tests**:
   ```bash
   make test
   ```

### Proto Files

The project includes comprehensive test proto definitions:

- **user.proto**: Basic message with nested Address and enum types
- **order.proto**: Order messages that reference User (tests imports)
- **nested.proto**: Complex nested scenarios including:
  - Deep nesting (4+ levels)
  - Maps with nested message values
  - oneOf with nested messages
  - Repeated nested messages
  - Optional/nullable nested messages
  - Real-world complex scenarios

## Testing

### Unit Tests

Fast tests with no external dependencies:

```bash
make test
# Or:
./gradlew test
```

The test suite includes:
- **SerializationTest**: Core serialization/deserialization logic
- **ProtobufDescriptorSetSerdeTest**: Main serde functionality
- **NestedMessagesTest**: Comprehensive nested message scenarios (8 tests)
- **TopicMappingTest**: Topic-to-message-type mapping

### Integration Tests

Tests requiring Docker and TestContainers:

```bash
# Run integration tests
ENABLE_INTEGRATION_TESTS=true ./gradlew integrationTest

# Or use Makefile commands
make integration-test        # Full integration test with local files
make s3-test                # Automated S3 integration test
make s3-test-start          # Interactive S3 test environment
```

### Test Categories

- **Unit Tests**: `src/test/java/**/*Test.java` (excluding `@IntegrationTest`)
- **Integration Tests**: Classes annotated with `@IntegrationTest`
- **S3 Tests**: TestContainers-based tests for S3 functionality

### Real-World Testing

The project includes comprehensive docker-compose setups for testing:

#### Local File Testing
```bash
make integration-test
# Access: http://localhost:8080
```

#### S3 Testing with MinIO
```bash
make s3-test-start
# Access: http://localhost:8081 (Kafka UI)
# Access: http://localhost:9001 (MinIO Console)
```

## Makefile Commands

### Building
```bash
make build                   # Build the project
make clean                   # Clean build artifacts
make jar                     # Build shadow jar only
make dev                     # Development build (compile + test)
make quick                   # Quick build (compile only, skip tests)
```

### Testing
```bash
make test                    # Run unit tests
make verify                  # Full verification (clean + build + check + format)
```

### Integration Testing
```bash
make integration-test        # Local file integration test
make integration-topics      # Create test topics only
make integration-test-message # Send test protobuf message
make integration-stop        # Stop integration test environment
make integration-clean       # Clean integration test environment

make s3-test                # Automated S3 integration test
make s3-test-start          # Interactive S3 test
make s3-test-stop           # Stop S3 test environment
make s3-test-clean          # Clean S3 test environment
make s3-test-logs           # View S3 test logs
make minio-logs             # View MinIO logs

make integration-test-full  # Both local and S3 tests
```

### Code Quality
```bash
make format                  # Apply code formatting
make format-check           # Check code formatting
```

### Proto Compilation
```bash
make proto-compile          # Compile proto files and generate descriptor set
```

## Architecture

### Core Components

1. **ProtobufDescriptorSetSerde**: Main serde implementation that integrates with Kafbat UI
2. **DescriptorSource**: Abstraction for loading descriptors from different sources
3. **DescriptorSourceFactory**: Factory pattern for creating appropriate source implementations
4. **ProtobufSerializer/Deserializer**: Handles protobuf serialization/deserialization
5. **DescriptorRefreshScheduler**: Background scheduler for refreshing descriptors from S3

### Source Implementations

- **LocalFileDescriptorSource**: Loads from local filesystem with file modification tracking
- **S3DescriptorSource**: Loads from S3-compatible storage with intelligent caching:
  - ETag-based change detection
  - Thread-safe caching with read/write locks
  - Configurable refresh intervals
  - Automatic retry and error handling

## CI/CD

The project uses a multi-platform CI/CD pipeline. Unit tests run on Linux, macOS, and Windows. Integration tests run only on Linux (Docker required).

## Contributing Guidelines

### Code Style

1. **Spotless formatting**: Code is automatically formatted using Google Java Format (AOSP style)
   - Run `make format` to format your code
   - CI will check formatting with `make format-check`
2. **Follow existing conventions**: Match the existing code style and patterns
3. **No unnecessary comments**: Code should be self-documenting
4. **Use existing libraries**: Check what's already available before adding new dependencies
5. **Error handling**: Provide meaningful error messages and proper exception handling

### Pull Request Process

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/your-feature-name`
3. **Make your changes**: Follow the code style guidelines
4. **Add tests**: Include both unit and integration tests as appropriate
5. **Update proto files if needed**: Run `make proto-compile` after changes
6. **Verify tests pass**: Run `make verify` to ensure all tests pass and code is formatted
7. **Update documentation**: Update relevant documentation (README, CONTRIBUTING, etc.)
8. **Submit pull request**: Include a clear description of changes

### Testing Requirements

- **Unit tests**: Required for all new functionality
- **Integration tests**: Required for S3-related features
- **Nested message tests**: Add test cases to NestedMessagesTest for complex scenarios
- **Real-world testing**: Test with docker-compose setup
- **Cross-platform**: Ensure compatibility across platforms

## Development Workflows

### Adding New Descriptor Sources

1. Implement `DescriptorSource` interface
2. Update `DescriptorSourceFactory` to create your source
3. Add `@IntegrationTest` test class
4. Update README.md with configuration examples

### Adding New Proto Test Cases

1. Edit proto file in `src/test/proto/`
2. Run `make proto-compile` to regenerate descriptor set
3. Add test case in `NestedMessagesTest.java`
4. Run `make test` to verify

### Debugging Integration Issues

```bash
make s3-test-logs                    # View S3 test logs
make minio-logs                      # View MinIO logs
docker-compose logs kafka-ui         # View Kafka UI logs
make s3-test-start                   # Start interactive S3 test
cd docker-compose && docker-compose exec kafka-ui /bin/bash  # Inspect container
```

## Release Process

1. Update version in `build.gradle` and README.md
2. Run full test suite: `make integration-test-full`
3. Build release: `make build`
4. Create GitHub release and attach shadow JAR

## Troubleshooting

### Common Development Issues

1. **Tests failing on macOS**: Integration tests require Docker and are disabled on macOS in CI
2. **JAR not found**: Ensure you've run `make build` to create the shadow JAR
3. **S3 connectivity**: Check MinIO is running and accessible
4. **Descriptor loading**: Verify descriptor file format and protobuf imports
5. **Proto compilation errors**: Ensure all proto imports are correct and files are in sync

### Debug Commands

```bash
ls -la build/libs/                              # Check built JARs
ls -la src/test/resources/test_descriptors.desc # Verify test descriptors
cd docker-compose && docker-compose ps          # Check services
docker-compose logs [service-name]              # Check logs
```

## Future Enhancements

Potential areas for contribution:

1. **Key Support**: Currently only VALUE target is supported (see ProtobufDescriptorSetSerde.java:251-252)
2. **IAM Role Support**: AWS IAM roles and temporary credentials
3. **Additional Sources**: HTTP endpoints, databases, Schema Registry
4. **Schema Evolution**: Support for schema compatibility checking
5. **Metrics**: Monitoring and observability features
6. **Performance**: Caching and optimization improvements

---

For questions about contributing, please open an issue or start a discussion on GitHub.
