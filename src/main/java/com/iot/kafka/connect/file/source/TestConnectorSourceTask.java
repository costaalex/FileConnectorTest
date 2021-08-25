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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * TestConnectorSourceTask reads from stdin or a file.
 */
public class TestConnectorSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(TestConnectorSourceTask.class);
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    BlockingQueue<SourceRecord> mqttRecordQueue = new LinkedBlockingQueue<SourceRecord>();

    private IMqttClient mqttClient;
    private String mqttTopic;
    private int batchSize;
    private String brokerAddress;
    private int brokerPort;
    private String clientId;
    //private Boolean cleanSession;
    private int connectionTimeout;
    private int keepAlive;
    private int qos;
    private String mqttUsername;
    private String mqttPassword;
    public TestConnectorSourceTask() { }

    @Override
    public String version() {
        return new TestConnectorSourceConnector().version();
    }

    private void initMqttClient() {

        MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setConnectionTimeout(connectionTimeout);
        mqttConnectOptions.setKeepAliveInterval(keepAlive);
        // mqttConnectOptions.setCleanSession(cleanSession);
        mqttConnectOptions.setUserName(mqttUsername);
        mqttConnectOptions.setPassword(mqttPassword.toCharArray());
        mqttConnectOptions.setAutomaticReconnect(true);

        try {
            mqttClient = new MqttClient(
                    String.format("tcp://%s:%d", brokerAddress, brokerPort),
                    clientId,
                    new MemoryPersistence());
            //mqttClient.setCallback(this);
            mqttClient.connect(mqttConnectOptions);
            logger.info("SUCCESSFULL MQTT CONNECTION for MqttSourceConnectorTask and mqtt client: '{}'.", mqttClient);
        } catch (MqttException e) {
            logger.error("FAILED MQTT CONNECTION for MqttSourceConnectorTask and mqtt client: '{}'.", mqttClient);
            logger.error(String.valueOf(e));
        }

       /* try {
            mqttClient.subscribe(mqttTopic, qos);
            logger.info("SUCCESSFULL MQTT CONNECTION for MqttSourceConnectorTask and mqtt client: '{}'.", mqttClient);
        } catch (MqttException e) {
            logger.error("FAILED MQTT CONNECTION for MqttSourceConnectorTask and mqtt client: '{}'.", mqttClient);
            e.printStackTrace();
        }*/

    }

    @Override
    public void start(Map<String, String> props) {

        // Missing topic or parsing error is not possible because we've parsed the config in the
        // Connector
        brokerAddress = props.get(TestConnectorSourceConnector.BROKER_ADDRESS);
        brokerPort = Integer.parseInt(props.get(TestConnectorSourceConnector.BROKER_PORT));
        mqttTopic = props.get(TestConnectorSourceConnector.TOPIC_CONFIG);
        batchSize = Integer.parseInt(props.get(TestConnectorSourceConnector.TASK_BATCH_SIZE_CONFIG));
        clientId = props.get(TestConnectorSourceConnector.CLIENT_ID);
        //cleanSession = Boolean.parseBoolean(props.get(v.CLEAN_SESSION));
        connectionTimeout = Integer.parseInt(props.get(TestConnectorSourceConnector.CONNECTION_TIMEOUT));
        keepAlive = Integer.parseInt(props.get(TestConnectorSourceConnector.KEEP_ALIVE));
        qos = Integer.parseInt(props.get(TestConnectorSourceConnector.QOS));
        mqttUsername = props.get(TestConnectorSourceConnector.MQTT_USERNAME);
        mqttPassword = props.get(TestConnectorSourceConnector.MQTT_PASSWORD);
        initMqttClient();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        try {
            if(!mqttRecordQueue.isEmpty())
            records.add(mqttRecordQueue.take());
        }catch (InterruptedException e){ }

        int i=0;
        if (records == null)
            records = new ArrayList<>();
        do{
            records.add(new SourceRecord(null, null, mqttTopic, null,
                    null, null, VALUE_SCHEMA, String.valueOf(System.currentTimeMillis()) , System.currentTimeMillis()));

            if (records.size() >= batchSize) {
                return records;
            }
            i++;
            //Thread.sleep(5);
        } while (i<1);

        return records;
    }

    @Override
    public void stop() {
        logger.trace("Stopping");
        synchronized (this) {
            this.notify();
        }
    }

}
