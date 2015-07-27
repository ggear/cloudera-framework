package com.cloudera.framework.main.test.cluster;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provide host network utilities
 */
public class HostNetwork {

  public static final String SERVER_BIND_IP = "127.0.0.1";
  public static final AtomicInteger SERVER_BIND_PORT_START = new AtomicInteger(25000);
  public static final int SERVER_BIND_PORT_FINISH = 25100;

  public static int getNextAvailablePort() throws IOException {
    while (SERVER_BIND_PORT_START.get() < SERVER_BIND_PORT_FINISH) {
      try {
        ServerSocket server = new ServerSocket(SERVER_BIND_PORT_START.getAndIncrement());
        server.close();
        return server.getLocalPort();
      } catch (IOException exception) {
        // ignore
      }
    }
    throw new IOException("Could not find avialable port");
  }

}
