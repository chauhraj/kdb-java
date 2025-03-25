package com.chauhraj.kdb.config;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;

public class ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);
    private final Config config;

    public ConfigurationLoader() {
        // Load configurations in order of precedence (lowest to highest)
        Config defaults = ConfigFactory.parseResources("defaults.conf");
        
        // Load environment specific config if it exists
        Config environment = ConfigFactory.empty();
        File envConfig = new File("config/environment.conf");
        if (envConfig.exists()) {
            environment = ConfigFactory.parseFile(envConfig);
        }
        
        // System properties have highest precedence
        Config system = ConfigFactory.systemProperties();
        
        // Combine configurations with proper fallback chain
        this.config = system
            .withFallback(environment)
            .withFallback(defaults)
            .resolve();
    }

    public void applyConfig(Bootstrap bootstrap, String scheme) {
        String commonPath = "netty.connection";
        String schemePath = commonPath + "." + scheme;
        
        if (!config.hasPath(commonPath)) {
            logger.warn("No common configuration found at path: {}", commonPath);
            return;
        }

        // Get common config
        Config commonConfig = config.getConfig(commonPath);
        
        // Get scheme specific config (if it exists)
        Config schemeConfig = config.hasPath(schemePath) ? 
            config.getConfig(schemePath).withFallback(commonConfig) : 
            commonConfig;
        
        // Apply configuration options
        if (schemeConfig.hasPath("keepAlive")) {
            bootstrap.option(ChannelOption.SO_KEEPALIVE, schemeConfig.getBoolean("keepAlive"));
        }
        if (schemeConfig.hasPath("tcpNoDelay")) {
            bootstrap.option(ChannelOption.TCP_NODELAY, schemeConfig.getBoolean("tcpNoDelay"));
        }
        if (schemeConfig.hasPath("connectTimeoutMillis")) {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, schemeConfig.getInt("connectTimeoutMillis"));
        }
        if (schemeConfig.hasPath("writeBufferHighWaterMark")) {
            bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, schemeConfig.getInt("writeBufferHighWaterMark"));
        }
        if (schemeConfig.hasPath("writeBufferLowWaterMark")) {
            bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, schemeConfig.getInt("writeBufferLowWaterMark"));
        }
    }

    public Config getConfig() {
        return config;
    }
} 