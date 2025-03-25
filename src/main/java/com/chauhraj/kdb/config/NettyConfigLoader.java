package com.chauhraj.kdb.config;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;

/**
 * Loads and applies Netty configuration from HOCON format
 */
public class NettyConfigLoader {
    private static final Logger logger = LoggerFactory.getLogger(NettyConfigLoader.class);
    private static final String CONFIG_BASE_PATH = "netty.client.";
    private static final Map<String, ChannelOption<?>> CHANNEL_OPTIONS_CACHE = new ConcurrentHashMap<>();
    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();
    
    private final Config config;

    public NettyConfigLoader() {
        this.config = ConfigFactory.load();
    }

    public NettyConfigLoader(Config config) {
        this.config = config;
    }

    /**
     * Applies configuration to a Bootstrap instance based on the connection scheme
     * @param bootstrap The Bootstrap instance to configure
     * @param scheme The connection scheme (tcp, epoll, uds, etc.)
     */
    @SuppressWarnings("unchecked")
    public void applyConfig(Bootstrap bootstrap, String scheme) {
        String configPath = CONFIG_BASE_PATH + scheme;
        if (!config.hasPath(configPath)) {
            logger.debug("No specific configuration found for scheme: {}", scheme);
            return;
        }

        Config schemeConfig = config.getConfig(configPath);
        schemeConfig.entrySet().forEach(entry -> {
            String optionName = entry.getKey();
            try {
                ChannelOption<?> option = getChannelOption(optionName);
                if (option != null) {
                    Object value = convertValue(entry.getValue().unwrapped(), option);
                    ((Bootstrap)bootstrap).option((ChannelOption<Object>) option, value);
                    logger.debug("Applied option {} with value {} for scheme {}", 
                               optionName, value, scheme);
                }
            } catch (Exception e) {
                logger.error("Failed to apply option: {} for scheme: {}", optionName, scheme, e);
            }
        });
    }

    private ChannelOption<?> getChannelOption(String optionName) {
        return CHANNEL_OPTIONS_CACHE.computeIfAbsent(optionName, key -> {
            try {
                Field field = ChannelOption.class.getField(key);
                return (ChannelOption<?>) field.get(null);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                logger.warn("Channel option not found: {}", key);
                return null;
            }
        });
    }

    private Object convertValue(Object configValue, ChannelOption<?> option) {
        try {
            Class<?> targetType = getOptionType(option);
            MethodHandle converter = LOOKUP.findStatic(
                targetType, 
                "valueOf", 
                MethodType.methodType(targetType, String.class)
            );
            return converter.invoke(configValue.toString());
        } catch (Throwable e) {
            // If conversion fails, return the original value
            return configValue;
        }
    }

    private Class<?> getOptionType(ChannelOption<?> option) {
        try {
            // Use reflection to get the generic type of the ChannelOption
            Field valueField = option.getClass().getDeclaredField("value");
            valueField.setAccessible(true);
            return valueField.getType();
        } catch (NoSuchFieldException e) {
            // Default to the config value's type if we can't determine the option type
            return Integer.class;
        }
    }
} 