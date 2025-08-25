# Contributing to Protobuf Descriptor Set Serde

Thank you for your interest in contributing! This guide covers development setup, testing, and contribution guidelines.

## Development Setup

### Prerequisites

- **Java 17+**: Required for building the project
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
   ./gradlew build
   ```

3. **Run tests**:
   ```bash
   # Unit tests (fast)
   ./gradlew test
   
   # Integration tests (requires Docker)
   ENABLE_INTEGRATION_TESTS=true ./gradlew integrationTest
   
   # Code formatting check
   ./gradlew spotlessCheck
   
   # Apply code formatting
   ./gradlew spotlessApply
   ```

## Project Structure

```
├── src/main/java/io/github/hursungyun/kafbat/ui/serde/
│   ├── ProtobufDescriptorSetSerde.java          # Main serde implementation
│   └── sources/                                  # Descriptor source abstractions
│       ├── DescriptorSource.java                 # Interface for all sources
│       ├── DescriptorSourceFactory.java          # Factory for creating sources
│       ├── LocalFileDescriptorSource.java        # Local file implementation
│       └── S3DescriptorSource.java              # S3/MinIO implementation
├── src/test/java/                               # Test files
├── docker-compose/                              # Integration testing
│   ├── docker-compose.yml                       # Test environment
│   ├── start-s3-integration-test.sh             # Interactive S3 test
│   └── test-s3-integration.sh                   # Automated S3 test
└── build.gradle                                 # Build configuration
```

## Testing

### Unit Tests

Fast tests with no external dependencies:

```bash
./gradlew test
```

### Integration Tests

Tests requiring Docker and TestContainers:

```bash
# Run integration tests
ENABLE_INTEGRATION_TESTS=true ./gradlew integrationTest

# Or use Makefile commands
make test                    # Unit tests only
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

```bash
# Building
make build                   # Build the project
make clean                   # Clean build artifacts
make jar                     # Build shadow jar only

# Testing  
make test                    # Run unit tests
make integration-test        # Local file integration test
make s3-test                # Automated S3 integration test
make s3-test-start          # Interactive S3 test
make integration-test-full  # Both local and S3 tests

# Development
make dev                     # Development build (compile + test)
make quick                   # Quick build (compile only, skip tests)
make verify                  # Full verification (clean + build + check)

# Maintenance
make s3-test-stop           # Stop S3 test environment
make s3-test-clean          # Clean S3 test environment
make s3-test-logs           # View S3 test logs
```

## Architecture

### Core Components

1. **ProtobufDescriptorSetSerde**: Main serde implementation that integrates with Kafbat UI
2. **DescriptorSource**: Abstraction for loading descriptors from different sources
3. **DescriptorSourceFactory**: Factory pattern for creating appropriate source implementations

### Source Implementations

- **LocalFileDescriptorSource**: Loads from local filesystem with file modification tracking
- **S3DescriptorSource**: Loads from S3-compatible storage with intelligent caching:
  - ETag-based change detection
  - Thread-safe caching with read/write locks
  - Configurable refresh intervals
  - Automatic retry and error handling

### Dependencies

**Core Dependencies:**
- `io.kafbat.ui:serde-api:1.0.0` - Kafbat UI serde API
- `com.google.protobuf:protobuf-java:3.24.4` - Protobuf runtime
- `io.minio:minio:8.5.7` - MinIO S3 client

**Test Dependencies:**
- `org.testcontainers:minio:1.19.3` - MinIO TestContainers
- `org.junit.jupiter:junit-jupiter:5.10.0` - JUnit 5

## CI/CD

### GitHub Actions Workflow

The project uses a multi-platform CI/CD pipeline:

- **Unit Tests**: Run on Linux, macOS, Windows
- **Integration Tests**: Run only on Linux (Docker required)
- **Build Artifacts**: Uploaded for each successful build

### Test Strategy

1. **Unit Tests**: Fast, isolated tests for core logic
2. **TestContainers**: Integration tests with real MinIO containers
3. **Docker Compose**: End-to-end testing with complete Kafka UI setup

## Contributing Guidelines

### Code Style

1. **Spotless formatting**: Code is automatically formatted using Google Java Format (AOSP style)
   - Run `./gradlew spotlessApply` to format your code
   - CI will check formatting with `./gradlew spotlessCheck`
2. **Follow existing conventions**: Match the existing code style and patterns
3. **No unnecessary comments**: Code should be self-documenting
4. **Use existing libraries**: Check what's already available before adding new dependencies
5. **Error handling**: Provide meaningful error messages and proper exception handling

### Pull Request Process

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/your-feature-name`
3. **Make your changes**: Follow the code style guidelines
4. **Add tests**: Include both unit and integration tests as appropriate
5. **Verify tests pass**: Run `make verify` to ensure all tests pass
6. **Update documentation**: Update relevant documentation
7. **Submit pull request**: Include a clear description of changes

### Testing Requirements

- **Unit tests**: Required for all new functionality
- **Integration tests**: Required for S3-related features
- **Real-world testing**: Test with docker-compose setup
- **Cross-platform**: Ensure compatibility across platforms

## Development Workflows

### Adding New Descriptor Sources

To add support for a new descriptor source (e.g., HTTP, database):

1. **Implement DescriptorSource interface**:
   ```java
   public class HttpDescriptorSource implements DescriptorSource {
       // Implementation
   }
   ```

2. **Update DescriptorSourceFactory**:
   ```java
   // Add factory logic for new source
   ```

3. **Add tests**:
   ```java
   @IntegrationTest
   class HttpDescriptorSourceTest {
       // TestContainers-based integration tests
   }
   ```

4. **Update documentation**: Add configuration examples

### Debugging Integration Issues

1. **Check logs**:
   ```bash
   make s3-test-logs     # S3 test logs
   make minio-logs       # MinIO logs
   ```

2. **Manual verification**:
   ```bash
   make s3-test-start    # Interactive mode
   # Then access http://localhost:8081
   ```

3. **Container inspection**:
   ```bash
   docker-compose exec kafka-ui-s3 /bin/bash
   docker-compose exec minio /bin/bash
   ```

## Release Process

1. **Update version**: Update version in `build.gradle`
2. **Run full test suite**: `make integration-test-full`
3. **Build release artifact**: `make build`
4. **Create release**: Tag and create GitHub release
5. **Upload JAR**: Attach shadow JAR to release

## Architecture Decisions

### Why MinIO for S3 Testing?

- **S3 Compatibility**: 100% S3 API compatible
- **TestContainers Support**: Well-supported in TestContainers ecosystem
- **Self-hosted Option**: Users can run their own S3-compatible storage
- **Development Friendly**: Easy to set up and debug locally

### Why Separate Source Abstractions?

- **Single Responsibility**: Each source handles one type of storage
- **Testability**: Easier to test individual source implementations
- **Extensibility**: Simple to add new source types
- **Configuration**: Clean separation of configuration concerns

## Troubleshooting

### Common Development Issues

1. **Tests failing on macOS**: Integration tests require Docker and are disabled on macOS in CI
2. **JAR not found**: Ensure you've run `./gradlew build` to create the shadow JAR
3. **S3 connectivity**: Check MinIO is running and accessible
4. **Descriptor loading**: Verify descriptor file format and protobuf imports

### Debug Commands

```bash
# Check built JARs
ls -la build/libs/

# Verify docker-compose services
docker-compose ps

# Check container logs
docker-compose logs [service-name]

# Test MinIO connectivity
docker-compose exec minio mc --version
```

## Code Organization

### Package Structure

- **Main Package**: `io.github.hursungyun.kafbat.ui.serde`
  - Contains main serde implementation
- **Sources Package**: `io.github.hursungyun.kafbat.ui.serde.sources`  
  - Contains descriptor source abstractions and implementations
- **Test Packages**: Mirror main package structure
  - Unit tests in same package as source
  - Integration tests with `@IntegrationTest` annotation

### Design Patterns

- **Factory Pattern**: `DescriptorSourceFactory` for creating sources
- **Strategy Pattern**: `DescriptorSource` interface with multiple implementations
- **Template Method**: Common caching and refresh logic in `S3DescriptorSource`

## Performance Considerations

### S3 Caching Strategy

- **ETag Validation**: Only download if object changed
- **Thread Safety**: Read/write locks for concurrent access
- **Memory Efficiency**: Cache descriptors in memory, not raw bytes
- **Configurable Refresh**: Balance between freshness and performance

### Resource Management

- **Connection Pooling**: MinIO client handles connection pooling
- **Cleanup**: Proper resource cleanup in tests and production code
- **Error Recovery**: Graceful handling of network failures

## Security Considerations

- **Credential Management**: S3 credentials in configuration only
- **Network Security**: Support for both HTTP and HTTPS endpoints
- **Access Control**: Respects S3 bucket permissions and IAM policies
- **No Credential Logging**: Sensitive information never logged

## Future Enhancements

Potential areas for contribution:

1. **IAM Role Support**: AWS IAM roles and temporary credentials
2. **Additional Sources**: HTTP endpoints, databases, etc.
3. **Schema Evolution**: Support for schema compatibility checking
4. **Metrics**: Monitoring and observability features
5. **Serialization**: Support for message serialization (currently read-only)

---

For questions about contributing, please open an issue or start a discussion on GitHub.