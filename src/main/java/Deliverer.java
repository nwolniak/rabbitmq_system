import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

public class Deliverer {
    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("Deliverer started");

        // connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();


        // channel
        Channel channel = connection.createChannel();


        // exchange
        String EXCHANGE_NAME = "exchangeOrders";
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // name of deliverer
        BufferedReader delivererNameBuffer = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter deliverer name:");
        String delivererName = delivererNameBuffer.readLine();

        // number of products offered
        BufferedReader productsNumberBuffer = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter the number of products offered:");
        int productsNumber = Integer.parseInt(productsNumberBuffer.readLine());


        // read product names
        ArrayList<String> productsOffered = new ArrayList<>();
        for (int i = 0; i < productsNumber; i++) {
            BufferedReader productNameBuffer = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter product name:");
            String productName = productNameBuffer.readLine();
            productsOffered.add(productName);
        }


        // create queue per product name
        ArrayList<String> queueNames = new ArrayList<>();
        for (String productName : productsOffered) {
            String QUEUE_NAME = "QUEUE_ORDERS_" + productName;
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "orders" + ".*." + productName);
            queueNames.add(QUEUE_NAME);
        }

        // queue & bind ADMIN
        String QUEUE_ADMIN_NAME = "QUEUE_" + delivererName;
        channel.queueDeclare(QUEUE_ADMIN_NAME, false, false, false, null);
        channel.queueBind(QUEUE_ADMIN_NAME, EXCHANGE_NAME, "deliverers");
        channel.queueBind(QUEUE_ADMIN_NAME, EXCHANGE_NAME, "all");

        // consumer (message handling)
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String key = envelope.getRoutingKey();
                String message = new String(body, "UTF-8");
                if (key.matches("orders(.*)")) {
                    String[] keyComponents = key.split("\\.");
                    String sendKey = "teams." + keyComponents[1];
                    String sendMsg = message + " Ack";
                    channel.basicPublish(EXCHANGE_NAME, sendKey, null, sendMsg.getBytes("UTF-8"));
                }

                System.out.println("Received: " + message);
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };


        // start listening Orders
        System.out.println("Waiting for orders...");

        // listening Administrator
        channel.basicConsume(QUEUE_ADMIN_NAME, false, consumer);

        // listening climbing teams
        for (String QUEUE_NAME : queueNames) {
            channel.basicConsume(QUEUE_NAME, false, consumer);
        }
    }
}
