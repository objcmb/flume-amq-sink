/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */
package com.flume.sink.amq;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;

import com.flume.amq.listener.ActiveMQTransportListener;

public class ActivemqSink extends AbstractSink implements Configurable {

	private JmsTemplate jmsTemplateQueue;
	private CachingConnectionFactory cachingConnectionFactory;
	private String brokerUrl;
	private String queueName;
	private SinkCounter counter;
	private int isTopic;
	private int batchSize;
	private static final Logger logger = LoggerFactory.getLogger(ActivemqSink.class);

	@Override
	public Status process() throws EventDeliveryException {
		logger.debug("Amq Sink {} process. Metrics: {}", getName(), counter);
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		try {

			Event event = channel.take();
			long processedEvents = 0L;
			while (event != null && processedEvents < batchSize) {
				String jsonEvent = new String(event.getBody(), ActivemqSinkConstants.UTF_8);
				logger.debug("send to amq :" + queueName + "," + jsonEvent);
				sendMsgToActiveMQ(queueName, jsonEvent);
				processedEvents++;
				event = channel.take();
			}
			if (processedEvents <= 0) {
				counter.incrementBatchEmptyCount();
				result = Status.BACKOFF;
			} else {
				counter.incrementBatchCompleteCount();
				// sinkCounter.addToEventDrainAttemptCount(processedEvents);
				processedEvents++;
				counter.addToEventDrainSuccessCount(processedEvents);
			}
			transaction.commit();
		} catch (Throwable e) {
			logger.error("sendMsgToActiveMQ error:", e);
			counter.incrementConnectionFailedCount();
			transaction.rollback();
		} finally {
			if (transaction != null) {
				transaction.close();
			}
		}

		return result;
	}

	@Override
	public synchronized void start() {
		// instantiate the producer
		try {
			counter.start();
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
			connectionFactory.setUseAsyncSend(true);
			connectionFactory.setProducerWindowSize(1024000);
			connectionFactory.getPrefetchPolicy().setQueuePrefetch(1);
			connectionFactory.setTransportListener(new ActiveMQTransportListener());
			PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory(connectionFactory);
			pooledConnectionFactory.setMaxConnections(10);
			pooledConnectionFactory.setMaximumActiveSessionPerConnection(20);
			cachingConnectionFactory = new CachingConnectionFactory(pooledConnectionFactory);
			cachingConnectionFactory.setSessionCacheSize(20);
			jmsTemplateQueue = new JmsTemplate(cachingConnectionFactory);
		} catch (Exception e) {
			e.printStackTrace();
		}
		super.start();
		logger.info("Amq Sink {} started. Metrics: {}", getName(), counter);
	}

	@Override
	public synchronized void stop() {
		cachingConnectionFactory.destroy();
		counter.stop();
		logger.info("Amq Sink {} stopped. Metrics: {}", getName(), counter);
		super.stop();
	}

	/**
	 * We configure the sink and generate properties for the AMQ Producer
	 *
	 * AMQ producer properties is generated as follows: 1. We generate a properties
	 * object with some static defaults that can be overridden by Sink configuration
	 * 2. We add the configuration users added for AMQ (parameters starting with
	 * .AMQ. and must be valid AMQ Producer properties 3. We add the sink's
	 * documented parameters which can override other properties
	 *
	 * @param context
	 */
	@Override
	public void configure(Context context) {
		try {
			if (counter == null) {
				counter = new SinkCounter(getName());
			}
			brokerUrl = context.getString(ActivemqSinkConstants.BROKER_URL);
			queueName = context.getString(ActivemqSinkConstants.QUEUE_NAME);
			isTopic = context.getInteger(ActivemqSinkConstants.IS_TOPIC, 0);
			batchSize = context.getInteger(ActivemqSinkConstants.BATCH_SIZE, 10);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sendMsgToActiveMQ(String destinationName, final String message) {
		jmsTemplateQueue.setPubSubDomain(isTopic == 1 ? true : false);
		jmsTemplateQueue.send(destinationName, new MessageCreator() {

			@Override
			public Message createMessage(Session session) throws JMSException {
				return session.createTextMessage(message);
			}
		});
	}
}
