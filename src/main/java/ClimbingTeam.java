import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class ClimbingTeam {
    public static void main(String[] args) throws IOException, TimeoutException {
        System.out.println("Climbing team started");

        // name of the climbing team
        BufferedReader climbingTeamNameBuffer = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter climbing team name:");
        String climbingTeamName = climbingTeamNameBuffer.readLine();

        // connection
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();

        // channel
        Channel channel = connection.createChannel();

        // exchange ORDERS
        String EXCHANGE_NAME = "exchangeOrders";
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // exchange ADMIN
//        String EXCHANGE_ADMIN_NAME = "exchangeAdministrator";
//        channel.exchangeDeclare(EXCHANGE_ADMIN_NAME, BuiltinExchangeType.TOPIC);

        // queue & bind ADMIN
        String QUEUE_NAME = "QUEUE_" + climbingTeamName;
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "teams");
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "all");
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "teams." + climbingTeamName);

        // consumer (message handling)
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received: " + message);
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        // listening
        channel.basicConsume(QUEUE_NAME, false, consumer);


        while (true) {
            // read msg
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("Enter order:");
            String orderMessage = br.readLine();

            // break condition
            if ("exit".equals(orderMessage)) {
                break;
            }

            // add orders tag
            String key = "orders." + climbingTeamName + "." + orderMessage;

            // publish
            channel.basicPublish(EXCHANGE_NAME, key, null, orderMessage.getBytes("UTF-8"));
            System.out.println("Sent: " + orderMessage);
        }
    }
}
