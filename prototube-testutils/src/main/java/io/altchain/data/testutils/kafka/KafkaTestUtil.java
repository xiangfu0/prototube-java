package io.altchain.data.testutils.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.security.JaasUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Properties;

public final class KafkaTestUtil {
  private static final int ZK_SESSION_TIMEOUT_MS = 10000;
  private static final int ZK_CONNECTION_TIMEOUT_MS = 10000;

  private KafkaTestUtil() {
  }

  static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find available port to use", e);
    }
  }

  public static boolean createKafkaTopicIfNecessary(String brokerUri, int replFactor, int numPartitions, String topic)
      throws IOException {
    URI zkUri = URI.create(brokerUri);
    String zkServerList = zkUri.getAuthority() + zkUri.getPath();

    ZkUtils zkUtils = ZkUtils.apply(zkServerList, ZK_SESSION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, JaasUtils
        .isZkSecurityEnabled());
    try {
      if (AdminUtils.topicExists(zkUtils, topic)) {
        return false;
      }

      try {
        AdminUtils.createTopic(zkUtils, topic, numPartitions, replFactor, new Properties(), null);
      } catch (TopicExistsException ignored) {
        return false;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    } finally {
      if (zkUtils != null) {
        zkUtils.close();
      }
    }
    return true;
  }
}
