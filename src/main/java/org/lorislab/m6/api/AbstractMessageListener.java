/*
 * Copyright 2019 lorislab.org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lorislab.m6.api;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.PostConstruct;
import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.inject.Inject;
import javax.jms.*;
import javax.ws.rs.core.MultivaluedMap;
import java.util.Enumeration;

/**
 * The abstract message listener.
 */
@Slf4j
public abstract class AbstractMessageListener<T extends Message> implements MessageListener {

    /**
     * The hyphen character replace string.
     */
    private static final String HYPHEN = "_HYPHEN_";

    /**
     * The default JMS context.
     */
    @Inject
    private JMSContext context;

    /**
     * The destination queue header property name.
     */
    private String destinationProperty;

    /**
     * The destination queue
     */
    private String destination;

    /**
     * The execution method muss be implemented be the project to process the message.
     *
     * @param input  the input message.
     * @param output the output message.
     */
    protected abstract void executeMessage(Message input, T output) throws JMSException;

    /**
     * The init method to load message flow.
     */
    @PostConstruct
    public void init() {
        MessageDriven tmp = this.getClass().getAnnotation(MessageDriven.class);
        if (tmp != null) {
            String queueName = null;

            if (tmp.activationConfig().length > 0) {
                for (ActivationConfigProperty acp : tmp.activationConfig()) {
                    log.info("ActivationConfigProperty {}:{}", acp.propertyName(), acp.propertyValue());
                    String value = acp.propertyValue();
                    if (!value.isEmpty()) {
                        switch (acp.propertyName()) {
                            case "destination":
                                queueName = value;
                                break;
                            case "nextDestination":
                                destination = value;
                                break;
                            case "nextDestinationProperty":
                                destinationProperty = value;
                                break;
                        }
                    }
                }
            }

            if (destination != null || destinationProperty != null) {

                log.info("Message flow {} flow {} -> {} or message property: {},", this.getClass().getSimpleName(), queueName, destination, destinationProperty);
            } else {
                log.warn("The message driven bean {} does not specified the nextDestination with the ActivationConfigProperty.", this.getClass().getSimpleName());
            }
        } else {
            log.warn("The message driven bean {} does not have annotation @MessageDriven", this.getClass().getSimpleName());
        }
    }

    /**
     * {@inheritDoc }
     */
    @Override
    @TransactionAttribute(TransactionAttributeType.REQUIRED)
    public void onMessage(Message input) {
        int redeliveryCount = 0;
        try {
            redeliveryCount = input.getIntProperty("JMSXDeliveryCount");
        } catch (JMSException ex) {
            log.error("Error reading the redelivery count property JMSXDeliveryCount", ex);
            throw new RuntimeException("Error reading the redelivery count property JMSXDeliveryCount", ex);
        }

        try {
            log.info("Execute message {}", input);

            // create and execute the message
            T output = createMessage(getContext(), input);
            executeMessage(input, output);

            if (output != null) {

                // get the next queue name
                String queueName = getNextQueueName(output, destination, destinationProperty);

                // send message to the next queue
                if (queueName != null) {

                    // copy header parameters
                    copyHeaderToOutput(input, output);

                    log.info("Message will be send to next queue {}", queueName);
                    Queue queue = getContext().createQueue(queueName);
                    getContext().createProducer().send(queue, output);
                } else {
                    log.info("No next queue specified. The flow of the message {} will end here.", input);
                }
            } else {
                log.info("The output message for the message {} is null!", input);
            }
        } catch (Exception ex) {
            log.error("Redelivery count: {}", redeliveryCount);
            throw new RuntimeException("Error processing the message!", ex);
        }
    }

    /**
     * Gets the current JMS context to send message to next destination.
     *
     * @return the current JMS context.
     */
    protected JMSContext getContext() {
        return context;
    }

    /**
     * Gets the next queue name base on the configuration in the @ActivationConfigProperty.
     *
     * @param output   the output message.
     * @param queue    the destination queue name.
     * @param property the destination queue name from this message property.
     * @return the corresponding queue name.
     * @throws JMSException if the method fails.
     */
    protected String getNextQueueName(Message output, String queue, String property) throws JMSException {
        String queueName = queue;
        if (queueName == null && property != null) {
            queueName = getHeader(output, property);
        }
        return queueName;
    }

    /**
     * Creates the message.
     *
     * @param context the JMS context.
     * @return the corresponding message.
     */
    @SuppressWarnings("unchecked")
    protected T createMessage(JMSContext context, Message input) {
        if (input instanceof TextMessage) {
            return (T) context.createTextMessage();
        }
        if (input instanceof MapMessage) {
            return (T) context.createMapMessage();
        }
        if (input instanceof BytesMessage) {
            return (T) context.createBytesMessage();
        }
        if (input instanceof StreamMessage) {
            return (T) context.createStreamMessage();
        }
        return (T) context.createObjectMessage();
    }

    /**
     * Copy or replace only key which does not exists in the output message or are M6_ prefix keys.
     *
     * @param input  the input message.
     * @param output the output message.
     * @throws Exception if the method fails.
     */
    static void copyHeaderToOutput(Message input, Message output) throws Exception {
        Enumeration keys = input.getPropertyNames();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            if (key != null && !output.propertyExists(key)) {
                Object value = input.getObjectProperty(key);
                output.setObjectProperty(key, value);
            }
        }
    }

    /**
     * Sets the message header property.
     *
     * @param message the message.
     * @param key     the property key.
     * @param value   the property value.
     * @throws JMSException if the method fails.
     * @see #createMessagePropertyName(String)
     */
    public static void setHeader(Message message, String key, Object value) throws JMSException {
        if (message != null && key != null) {
            message.setObjectProperty(createMessagePropertyName(key), value);
        }
    }

    /**
     * Gets the header property from the message.
     *
     * @param message the message.
     * @param key     the message key.
     * @param <T>     the property value type.
     * @return the property value.
     * @throws JMSException if the method fails.
     * @see #createMessagePropertyName(String)
     */
    @SuppressWarnings("unchecked")
    protected static <T> T getHeader(Message message, String key) throws JMSException {
        String property = createMessagePropertyName(key);
        if (property != null && !property.isEmpty()) {
            if (message.propertyExists(property)) {
                return (T) message.getObjectProperty(property);
            }
        }
        return null;
    }

    /**
     * Creates JMS compatible key. Replace all '-' hyphen characters by text '_HYPHEN'
     *
     * @param input the key.
     * @return the compatible JMS key.
     */
    public static String createMessagePropertyName(String input) {
        if (input != null && !input.isEmpty()) {
            return input.replaceAll("-", HYPHEN);
        }
        return input;
    }

    /**
     * Copy the header parameters from the multi-value map (http request header) to the message.
     *
     * @param map     the multi-value map.
     * @param message the message.
     * @throws JMSException if the method fails.
     */
    public static void copyHeader(MultivaluedMap<String, String> map, Message message) throws JMSException {
        if (map != null && message != null) {
            for (String key : map.keySet()) {
                String value = map.getFirst(key);
                setHeader(message, key, value);
            }
        }
    }
}
