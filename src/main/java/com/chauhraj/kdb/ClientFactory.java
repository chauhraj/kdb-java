package com.chauhraj.kdb;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketChannel;

/**
 * Factory class for creating KDB+ client instances based on different connection schemes.
 * Supported schemes:
 * - tcp://host:port    (Standard TCP connection)
 * - epoll://host:port  (Linux epoll-based connection)
 * - kqueue://host:port (BSD/MacOS kqueue-based connection)
 * - uds:///path/to/socket (Unix Domain Socket connection)
 */
public class ClientFactory {
  
  private static final Logger logger = LoggerFactory.getLogger(ClientFactory.class);

  private ClientFactory() {
    // Prevent instantiation
  }

  /**
   * Creates and connects a Client instance based on the provided URL.
   *
   * @param url Connection URL in the format scheme://host:port or uds:///path/to/socket
   * @return A connected Client instance
   * @throws IllegalArgumentException if the URL is malformed or unsupported
   * @throws RuntimeException if connection fails
   */
  public static Client createClient(String url) {
    try {
      URI uri = new URI(url);
      String scheme = validateAndGetScheme(uri);
      Class<? extends Channel> channelClass = getChannelClass(scheme);

      if ("uds".equals(scheme)) {
        validateUnixDomainSocket(uri);
      } else {
        validateHostPort(uri);
      }

      return new Client.ClientBuilder(uri, scheme, channelClass).build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URL format: " + url, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Connection interrupted", e);
    }
  }

  private static String validateAndGetScheme(URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      throw new IllegalArgumentException("URL scheme must be specified");
    }
    return scheme.toLowerCase();
  }

  private static Class<? extends Channel> getChannelClass(String scheme) {
    return switch (scheme) {
      case "epoll"  -> EpollSocketChannel.class;
      case "kqueue" -> KQueueSocketChannel.class;
      case "uds"    -> DomainSocketChannel.class;
      case "tcp"    -> NioSocketChannel.class;
      default       -> throw new IllegalArgumentException(
        "Unsupported scheme: " + scheme + ". Supported schemes are: tcp, epoll, kqueue, uds"
      );
    };
  }

  private static void validateHostPort(URI uri) {
    if (uri.getHost() == null || uri.getPort() == -1) {
      throw new IllegalArgumentException(
        "Invalid URL format. Expected: scheme://host:port, got: " + uri);
    }
  }

  private static void validateUnixDomainSocket(URI uri) {
    if (uri.getPath() == null || uri.getPath().isEmpty()) {
      throw new IllegalArgumentException(
        "Invalid Unix Domain Socket path. Expected: uds:///path/to/socket, got: " + uri);
    }
    Path socketPath = Paths.get(uri.getPath());
    if (!socketPath.isAbsolute()) {
      throw new IllegalArgumentException(
        "Unix Domain Socket path must be absolute: " + uri.getPath());
    }
  }
}