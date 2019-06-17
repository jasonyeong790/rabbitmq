package com.rabbitmq;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.MessageProperties;

// https://www.rabbitmq.com/tutorials/tutorial-one-java.html

public class RabbitMQTest {

	
	private final static String CONNECTION_HOST = "localhost";	
	private static final String EXCHANGE_NAME = "logs";
	private final static String TASK_QUEUE_NAME = "task_queue";
    private final static String QUEUE_NAME = "hello";

    
    // -----------------------------------------------------------------------------------
    public static void main(String[] argv) throws IOException, TimeoutException {
    	
    	 
    	//sendMessage();
    	//receiveMessage();
    	
    	//newTask(argv);
    	//Worker();
    	
    	//EmitLog(argv);
    	//ReceiveLogs();
    }	
    // -----------------------------------------------------------------------------------
    

    
    // -----------------------------------------------------------------------------------
    public static Channel createNewChannel() throws IOException, TimeoutException {
    	ConnectionFactory factory = new ConnectionFactory();
    	factory.setHost(CONNECTION_HOST);
    	Connection connection = factory.newConnection();    	
    	return connection.createChannel();
    }
    // -----------------------------------------------------------------------------------
    
    
    
    
    
    // -----------------------------------------------------------------------------------
    public static void EmitLog(String[] argv) throws IOException, TimeoutException {
    	// The most important change is that we now want to publish messages to our logs exchange instead of the nameless one.
    	Channel channel = createNewChannel();
    	channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    	String message = argv.length < 1 ? "info: Hello World!" :
                             String.join(" ", argv);
    	channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
    	System.out.println(" [x] Sent '" + message + "'");
    }
    // -----------------------------------------------------------------------------------
    public static void ReceiveLogs() throws IOException, TimeoutException {
    	Channel channel = createNewChannel();
    	channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    	String queueName = channel.queueDeclare().getQueue();
    	channel.queueBind(queueName, EXCHANGE_NAME, "");
    	
    	System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    	
    	DeliverCallback deliverCallback = (consumerTag, delivery) -> {
    		String message = new String(delivery.getBody(), "UTF-8");
    		System.out.println(" [x] Received '" + message + "'");
    	};
    	
    	channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
    // -----------------------------------------------------------------------------------
    
    
    
	// -----------------------------------------------------------------------------------
    public static void newTask(String[] argv) throws IOException, TimeoutException {
    	// We will slightly modify the Send.java code from our previous example, to allow arbitrary messages to be sent from the command line. 
    	// This program will schedule tasks to our work queue, so let's name it  newTask
    	Channel channel = createNewChannel();
    	String message = String.join(" ", argv);
    	
    	// Now we need to mark our messages as persistent - by setting MessageProperties (which implements BasicProperties) to the value PERSISTENT_TEXT_PLAIN.
    	// Marking messages as persistent doesn't fully guarantee that a message won't be lost. 
    	// The persistence guarantees aren't strong, but it's more than enough for our simple task queue. 
    	// If you need a stronger guarantee then you can use publisher confirms.
    	channel.basicPublish("", TASK_QUEUE_NAME,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes());
    	
    	System.out.println(" [x] Sent '" + message + "'");
    }
    // -----------------------------------------------------------------------------------
    public static void Worker() throws IOException, TimeoutException {
   
    	// Our old Recv.java program also requires some changes: it needs to fake a second of work for every dot in the message body. 
    	// It will handle delivered messages and perform the task, so let's call it
    	Channel channel = createNewChannel();
        channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        
        // In order to defeat that we can use the basicQos method with the prefetchCount = 1 setting. 
        // This tells RabbitMQ not to give more than one message to a worker at a time. 
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);
        
        // We explicitly turned them off via the autoAck=true flag. 
        // It's time to set this flag to false and send a proper acknowledgment from the worker, once we're done with a task.
    	DeliverCallback deliverCallback = (consumerTag, delivery) -> {
    		  String message = new String(delivery.getBody(), "UTF-8");

    		  System.out.println(" [x] Received '" + message + "'");
    		  try {
    		    doWork(message);
    		  } catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
    		    System.out.println(" [x] Done");
    		    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    		  }
    		};
    		
    		boolean durable = true;
    		channel.basicConsume(TASK_QUEUE_NAME, durable, deliverCallback, consumerTag -> { });
    }
    // -----------------------------------------------------------------------------------
    private static void doWork(String task) throws InterruptedException {
    	// Our fake task to simulate execution time:
    	 for (char ch : task.toCharArray()) {
    		 if (ch == '.') {
    			 try {
    				 Thread.sleep(1000);
   	            } catch (InterruptedException _ignored) {
   	                Thread.currentThread().interrupt();
   	            }
   	        }
   	    }
    }
    // -----------------------------------------------------------------------------------
    
    
    
    
    // -----------------------------------------------------------------------------------
    // We'll call our message publisher (sender) Send and our message consumer (receiver) Recv. 
    // The publisher will connect to RabbitMQ, send a single message, then exit.
    public static void sendMessage() throws IOException, TimeoutException {
    	Channel channel = createNewChannel();
    	channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    	String message = "Hello World!";
    	channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
    	System.out.println(" [x] Sent '" + message + "'");
    }
    // -----------------------------------------------------------------------------------
    public static void receiveMessage() throws IOException, TimeoutException {
    	Channel channel = createNewChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
        };
        
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
    }
    // -----------------------------------------------------------------------------------
    
    
    
    
} 