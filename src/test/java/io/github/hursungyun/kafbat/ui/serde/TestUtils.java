package io.github.hursungyun.kafbat.ui.serde;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.TestcontainersConfiguration;

/**
 * Utility class for test-related functionality
 */
public class TestUtils {
    
    /**
     * Check if Docker is available for TestContainers
     * @return true if Docker is available and working
     */
    public static boolean isDockerAvailable() {
        try {
            // Check if TestContainers can detect Docker
            DockerClientFactory.instance().client();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * Check if integration tests should be enabled
     * @return true if integration tests should run
     */
    public static boolean areIntegrationTestsEnabled() {
        return "true".equals(System.getenv("ENABLE_INTEGRATION_TESTS")) || 
               "true".equals(System.getProperty("enableIntegrationTests")) ||
               isDockerAvailable();
    }
}