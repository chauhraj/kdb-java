package com.chauhraj.kdb;

import java.io.Closeable;
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chauhraj.kdb.config.ConfigurationLoader;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

public class Client implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Client.class);
    
    private final Channel channel;
    private final EventLoopGroup group;
    
    /**
     * Private constructor following RAII pattern - establishes connection during initialization
     */
    private Client(Bootstrap bootstrap, URI uri, String scheme) throws InterruptedException {
        ChannelFuture future;
        if ("uds".equals(scheme)) {
            future = bootstrap.connect(new DomainSocketAddress(uri.getPath())).sync();
        } else {
            future = bootstrap.connect(uri.getHost(), uri.getPort()).sync();
        }
        
        this.channel = future.channel();
        this.group = bootstrap.config().group();
        logger.info("Connected to {} using scheme {}", uri, scheme);
    }

    public void sendMessage(String message) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(message);
            logger.debug("Sent message: {}", message);
        } else {
            throw new IllegalStateException("Channel is not active");
        }
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close().sync();
                logger.info("Channel closed");
            }
        } catch (InterruptedException e) {
            logger.error("Error while closing channel", e);
            Thread.currentThread().interrupt();
        } finally {
            if (group != null) {
                group.shutdownGracefully();
                logger.info("EventLoopGroup shutdown");
            }
        }
    }

    /**
     * Builder class for Client to handle the connection setup
     */
    static class ClientBuilder {
        private final URI uri;
        private final String scheme;
        private final EventLoopGroup group;
        private final Bootstrap bootstrap;
        private final ConfigurationLoader configLoader;

        ClientBuilder(URI uri, String scheme, Class<? extends Channel> channelClass) {
            this.uri = uri;
            this.scheme = scheme;
            this.group = new NioEventLoopGroup();
            this.bootstrap = new Bootstrap();
            this.configLoader = new ConfigurationLoader();
            
            bootstrap.group(group)
                    .channel(channelClass)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(
                                new StringDecoder(),
                                new StringEncoder(),
                                new ClientHandler()
                            );
                        }
                    });
            
            // Apply configuration from the hierarchical config system
            configLoader.applyConfig(bootstrap, scheme);
        }

        Client build() throws InterruptedException {
            return new Client(bootstrap, uri, scheme);
        }
    }
} 