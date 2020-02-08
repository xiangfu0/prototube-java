package io.altchain.data.testutils.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;

public class EmbeddedZooKeeper implements Closeable {
  private static final int TICK_TIME = 500;
  private final NIOServerCnxnFactory factory;
  private final ZooKeeperServer zookeeper;
  private final File tmpDir;
  private final int port;

  EmbeddedZooKeeper() throws IOException, InterruptedException {
    this.tmpDir = Files.createTempDirectory(null).toFile();
    this.factory = new NIOServerCnxnFactory();
    this.zookeeper = new ZooKeeperServer(new File(tmpDir, "data"), new File(tmpDir, "log"), TICK_TIME);
    InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 0);
    factory.configure(addr, 0);
    factory.startup(zookeeper);
    this.port = zookeeper.getClientPort();
  }

  public int getPort() {
    return port;
  }

  @Override
  public void close() throws IOException {
    zookeeper.shutdown();
    factory.shutdown();
    FileUtils.deleteDirectory(tmpDir);
  }
}
