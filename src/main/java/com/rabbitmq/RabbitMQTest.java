package com.rabbitmq;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.MessageProperties;

// https://www.rabbitmq.com/tutorials/tutorial-one-java.html

public class RabbitMQTest {

	
	private final static String CONNECTION_HOST = "localhost";	
	private static final String EXCHANGE_NAME = "logs";
	private static final String EXCHANGE_NAME2 = "topic_logs";
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
    	
    	//EmitLogTopic(argv);
    	//ReceiveLogsTopic(argv);
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
    // Messages sent to a topic exchange can't have an arbitrary routing_key - it must be a list of words, delimited by dots. 
    // The words can be anything, but usually they specify some features connected to the message. 
    // A few valid routing key examples: "stock.usd.nyse", "nyse.vmw", "quick.orange.rabbit". There can be as many words in the routing key as you like, up to the limit of 255 bytes.
    public static void EmitLogTopic(String[] argv) throws UnsupportedEncodingException, IOException, TimeoutException {
    	Channel channel = createNewChannel();
    	channel.exchangeDeclare(EXCHANGE_NAME2, "topic");

        String routingKey = getRouting(argv);
        String message = getMessage(argv);

        channel.basicPublish(EXCHANGE_NAME2, routingKey, null, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
    }
    // -----------------------------------------------------------------------------------
    public static void ReceiveLogsTopic(String [] argv) throws IOException, TimeoutException {
    	Channel channel = createNewChannel();
    	channel.exchangeDeclare(EXCHANGE_NAME2, "topic");
        String queueName = channel.queueDeclare().getQueue();

        if (argv.length < 1) {
            System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
            System.exit(1);
        }

        for (String bindingKey : argv) {
            channel.queueBind(queueName, EXCHANGE_NAME2, bindingKey);
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" +
                delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
    }
    // -----------------------------------------------------------------------------------
  
   
    
    
    
    // -----------------------------------------------------------------------------------
    // We want to extend that to allow filtering messages based on their severity. 
    // We will use a direct exchange instead. The routing algorithm behind a direct exchange is simple - 
    // a message goes to the queues whose binding key exactly matches the routing key of the message.
    // We'll use this model for our logging system. Instead of fanout we'll send messages to a direct exchange. 
    // We will supply the log severity as a routing key. That way the receiving program will be able to select the severity it wants to receive.
    public static void EmitLogDirect(String[] argv) throws IOException, TimeoutException {
    	Channel channel = createNewChannel();
    	channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String severity = getSeverity(argv);
        String message = getMessage(argv);

        channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
        System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
    }
    // -----------------------------------------------------------------------------------
    public static void ReceiveLogsDirect(String [] argv) throws IOException, TimeoutException {
    	Channel channel = createNewChannel();
    	String queueName = channel.queueDeclare().getQueue();

        if (argv.length < 1) {
        	System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
        	System.exit(1);
        }

        for (String severity : argv) {
        	channel.queueBind(queueName, EXCHANGE_NAME, severity);
        }
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
    // -----------------------------------------------------------------------------------
    
    
    
    
    
    // -----------------------------------------------------------------------------------
    // The most important change is that we now want to publish messages to our logs exchange instead of the nameless one.
    public static void EmitLog(String[] argv) throws IOException, TimeoutException {
    	
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
    // We will slightly modify the Send function from our previous example, to allow arbitrary messages to be sent from the command line. 
 	// This program will schedule tasks to our work queue, so let's name it  newTask
    public static void newTask(String[] argv) throws IOException, TimeoutException {
    	
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
    // Our old Recv function also requires some changes: it needs to fake a second of work for every dot in the message body. 
	// It will handle delivered messages and perform the task, so let's call it
    public static void Worker() throws IOException, TimeoutException {
   
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
    // Our fake task to simulate execution time:
    private static void doWork(String task) throws InterruptedException {
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
    
    
    
    
    
    // -----------------------------------------------------------------------------------
    private static String getRouting(String[] strings) {
        if (strings.length < 1)
            return "anonymous.info";
        return strings[0];
    }
    // -----------------------------------------------------------------------------------
    private static String getSeverity(String[] strings) {
        if (strings.length < 1)
            return "info";
        return strings[0];
    }
    // -----------------------------------------------------------------------------------
    private static String getMessage(String[] strings) {
        if (strings.length < 2)
            return "Hello World!";
        return joinStrings(strings, " ", 1);
    }
    // -----------------------------------------------------------------------------------
    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0) return "";
        if (length <= startIndex) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
    // -----------------------------------------------------------------------------------
    
    
    
} 