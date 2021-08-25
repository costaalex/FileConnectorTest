/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iot.kafka.connect.file.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.*;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class TestConnectorSourceConnector extends SourceConnector {
    public static final String TOPIC_CONFIG = "topic";
    public static final String TASK_BATCH_SIZE_CONFIG = "batch.size";

    public static final int DEFAULT_TASK_BATCH_SIZE = 2000;

    public static final String BROKER_ADDRESS = "broker_address";

    //PORT of the target MQTT Broker
    public static final String BROKER_PORT = "broker_port";

    //MQTT account username to connect to the target broker
    public static final String MQTT_USERNAME = "username";

    //MQTT account password to connect to the target broker
    public static final String MQTT_PASSWORD = "password";

    public static final String QOS = "qos";

    public static final String CLEAN_SESSION = "clean_session";

    public static final String CONNECTION_TIMEOUT = "connection_timeout";

    public static final String KEEP_ALIVE = "keep_alive";

    public static final String CLIENT_ID = "client_id";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(BROKER_ADDRESS, Type.STRING, "127.0.0.1", Importance.HIGH,
                    "broker address")
            .define(BROKER_PORT, Type.INT, 1883, Importance.HIGH,
                    "broker port")
            .define(TOPIC_CONFIG, Type.LIST, Importance.HIGH, "The topic to publish data to")
            .define(CLIENT_ID, Type.STRING, UUID.randomUUID().toString(), Importance.MEDIUM,
                    "mqtt client id to use don't set to use random")
            .define(CLEAN_SESSION, Type.INT, 1, Importance.MEDIUM,
                    "If connection should begin with clean session")
            .define(CONNECTION_TIMEOUT, Type.INT, 30, Importance.LOW,
                    "Connection timeout limit")
            .define(KEEP_ALIVE, Type.INT, 60, Importance.LOW,
                    "The interval to keep alive")
            .define(QOS, Type.INT, 2, Importance.LOW,
                    "which qos to use for paho client connection")
            .define(MQTT_USERNAME, Type.STRING, "", Importance.HIGH,
                    "which qos to use for paho client connection")
            .define(MQTT_PASSWORD, Type.STRING, "", Importance.HIGH,
                    "which qos to use for paho client connection")
            .define(TASK_BATCH_SIZE_CONFIG, Type.INT, DEFAULT_TASK_BATCH_SIZE, Importance.LOW,
                    "The maximum number of records the Source task can read from file one time");

    private String brokerAddress;
    private int brokerPort;
    private String topic;
    private int batchSize;
    private String clientId;
    private int cleanSession;
    private int connectionTimeout;
    private int keepAlive;
    private int qos;
    private String mqttUsername;
    private String mqttPassword;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
        //filename = parsedConfig.getString(FILE_CONFIG);
        List<String> topics = parsedConfig.getList(TOPIC_CONFIG);
        if (topics.size() != 1) {
            throw new ConfigException("'topic' in TestConnectorSourceConnector configuration requires definition of a single topic");
        }
        topic = topics.get(0);
        batchSize = parsedConfig.getInt(TASK_BATCH_SIZE_CONFIG);
        brokerAddress = parsedConfig.getString(BROKER_ADDRESS);
        brokerPort = parsedConfig.getInt(BROKER_PORT);
        clientId = parsedConfig.getString(CLIENT_ID);
        cleanSession = parsedConfig.getInt(CLEAN_SESSION);
        connectionTimeout = parsedConfig.getInt(CONNECTION_TIMEOUT);
        keepAlive = parsedConfig.getInt(KEEP_ALIVE);
        qos = parsedConfig.getInt(QOS);
        mqttUsername = parsedConfig.getString(MQTT_USERNAME);
        mqttPassword = parsedConfig.getString(MQTT_PASSWORD);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TestConnectorSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<>();
        //if (filename != null)
        //    config.put(FILE_CONFIG, filename);
        config.put(TOPIC_CONFIG, topic);
        config.put(TASK_BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        config.put(BROKER_ADDRESS, brokerAddress);
        config.put(BROKER_PORT, String.valueOf(brokerPort));
        config.put(CLIENT_ID, clientId);
        config.put(CLEAN_SESSION, String.valueOf(cleanSession));
        config.put(CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
        config.put(KEEP_ALIVE, String.valueOf(keepAlive));
        config.put(QOS, String.valueOf(qos));
        config.put(MQTT_USERNAME, String.valueOf(mqttUsername));
        config.put(MQTT_PASSWORD, String.valueOf(mqttPassword));
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to do since TestConnectorSourceConnector has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
