import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class Administrator {
    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("Administrator started");

        // connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();


        // channel
        Channel channel = connection.createChannel();

        // exchange
        String EXCHANGE_NAME = "exchangeOrders";
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // queue & bind
        String QUEUE_NAME = "QUEUE_Administrator";
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "orders.#");
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "teams.#");

        // consumer (message handling)
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message);
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume(QUEUE_NAME, false, consumer);


        while (true) {
            // read key
            BufferedReader keyBuffer = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter key [teams, deliverers, all]:");
            String key = keyBuffer.readLine();

            // read msg
            BufferedReader messageBuffer = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter message:");
            String message = messageBuffer.readLine();

            // break condition
            if ("exit".equals(message)) {
                break;
            }

            // publish
            channel.basicPublish(EXCHANGE_NAME, key, null, message.getBytes("UTF-8"));
            System.out.println("Sent: " + message);
        }
    }
}
