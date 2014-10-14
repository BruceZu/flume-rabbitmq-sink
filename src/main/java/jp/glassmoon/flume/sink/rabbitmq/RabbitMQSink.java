/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package jp.glassmoon.flume.sink.rabbitmq;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import static jp.glassmoon.flume.sink.rabbitmq.RabbitMQSink.Constants.*;

/**
 * A sink class to RabbitMQ.
 *
 * @author rinrinne (rinrin.ne@gmail.com)
 */
public class RabbitMQSink extends AbstractSink implements Configurable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQSink.class);

    private ConnectionFactory factory;
    private Connection connection;

    private String exchangeName;
    private int deliveryMode;
    private int priority;
    private String routingKey;

    private String appId;
    private String contentEncoding;
    private String contentType;

    private CounterGroup counterGroup;

    /**
     * Default constructor.
     */
    public RabbitMQSink() {
        counterGroup = new CounterGroup();
    }

    @Override
    public void configure(Context context) {
        try {
            factory = new ConnectionFactory();
            factory.setUri(context.getString(URI, DEFAULT_URI));
            factory.setUsername(context.getString(USERNAME, DEFAULT_USERNAME));
            factory.setPassword(context.getString(PASSWORD, DEFAULT_PASSWORD));
        } catch (Exception ex) {
            LOGGER.warn("Wrong configuration", ex);
            factory = null;
        }

        exchangeName = context.getString(EXCHANGE, DEFAULT_EXCHANGE);
        deliveryMode = context.getInteger(DELIVERYMODE, DEFAULT_DELIVERYMODE);
        priority = context.getInteger(PRIORITY, DEFAULT_PRIORITY);
        routingKey = context.getString(ROUTINGKEY, DEFAULT_ROUTINGKEY);

        appId = context.getString(APPID, DEFAULT_APPID);
        contentEncoding = context.getString(CONTENT_ENCODING, DEFAULT_CONTENT_ENCODING);
        contentType = context.getString(CONTENT_TYPE, DEFAULT_CONTENT_TYPE);
    }

    /**
     * Creates connection to RabbitMQ.
     *
     * @throws IOException throws if so.
     */
    private void createConnection() throws IOException {
        if (connection == null) {
            LOGGER.debug("Creating connection to host: {} port: {} vhost: {}, username: {}",
                    factory.getHost(), factory.getPort(), factory.getVirtualHost(), factory.getUsername());
            connection = factory.newConnection();
        }
    }

    /**
     * Destroy connection.
     */
    private void destroyConnection() {
        if (connection != null) {
            try {
                LOGGER.debug("Destroying connection to host: {} port] {}", factory.getHost(), factory.getPort());
                connection.close();
            } catch (Exception ex) {
                LOGGER.error("Unable to close connection.", ex);
            }
        }
        connection = null;
    }

    @Override
    public synchronized void start() {
        if (factory != null) {
            LOGGER.info("RabbitMQ sink starting");
            try {
                createConnection();
            } catch (Exception ex) {
                LOGGER.error("Unable to create connection using host: {} port: {} vhost: {} username: {}",
                        factory.getHost(), factory.getPort(), factory.getVirtualHost(), factory.getUsername());
                LOGGER.error("Exception", ex);
                destroyConnection();
            }
        }
        super.start();

        LOGGER.debug("RabbitMQ sink {} started.", this.getName());
    }

    @Override
    public synchronized void stop() {
        LOGGER.info("RabbitMQ sink {} stopping", this.getName());
        destroyConnection();
        LOGGER.debug("RabbitMQ sink {} stopped. Metrics: {}", this.getName(), counterGroup);
        super.stop();
    }

    /**
     * Sends messages to RabbitMQ.
     *
     * @param event the event.
     */
    private void sendMessage(Event event) throws IOException {
        if (connection != null) {
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.putAll(event.getHeaders());

            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
            .appId(appId)
            .contentEncoding(contentEncoding)
            .contentEncoding(contentType)
            .priority(priority)
            .deliveryMode(deliveryMode)
            .headers(headers);

            com.rabbitmq.client.Channel channel = connection.createChannel();
            channel.basicPublish(exchangeName, routingKey, builder.build(), event.getBody());
            channel.close();
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();

        try {
            transaction.begin();
            createConnection();
            Event event = channel.take();

            if (event == null) {
                counterGroup.incrementAndGet("event.empty");
                status = Status.BACKOFF;
            } else {
                sendMessage(event);
                counterGroup.incrementAndGet("event.rabbitmq");
            }
            transaction.commit();
        } catch (ChannelException cex) {
            transaction.rollback();
            LOGGER.error("Unable to get event from channel. Exception follows.", cex);
            status = Status.BACKOFF;
        } catch (Exception ex) {
            transaction.rollback();
            LOGGER.error("Unable to communicate with RabbitMQ server. Exception follows.", ex);
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }
        return status;
    }

    /**
     * A static class for constants
     *
     * @author rinrinne (rinrin.ne@gmail.com)
     */
    public static class Constants {
        public static final String URI = "uri";
        public static final String USERNAME = "username";
        public static final String PASSWORD = "password";
        public static final String EXCHANGE = "exchange";
        public static final String DELIVERYMODE = "deliveryMode";
        public static final String PRIORITY = "priority";
        public static final String ROUTINGKEY = "routingKey";
        public static final String APPID = "appId";
        public static final String CONTENT_ENCODING = "contentEncoding";
        public static final String CONTENT_TYPE = "contentType";

        public static final String DEFAULT_URI = "amqp://localhost";
        public static final String DEFAULT_USERNAME = "guest";
        public static final String DEFAULT_PASSWORD = "guest";
        public static final String DEFAULT_EXCHANGE = "gerrit.publish";
        public static final int DEFAULT_DELIVERYMODE = 1;
        public static final int DEFAULT_PRIORITY = 0;
        public static final String DEFAULT_ROUTINGKEY = "";
        public static final String DEFAULT_APPID = "";
        public static final String DEFAULT_CONTENT_ENCODING = "UTF-8";
        public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    }
}
