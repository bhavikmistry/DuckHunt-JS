package africa.absa.ursa.minor;

import africa.absa.absaoss.kafkarest.model.DuckHuntEvent;
import africa.absa.absaoss.kafkarest.model.EventType;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static java.lang.System.out;

/**
 * Hello world!
 *
 */
public class App implements Runnable
{
    public App(String email) {
        this.email = email;
    }

    public static void main(String[] args ) {
        for (int i = 0; i < 100; i++) {
            Thread thread = new Thread(new App("email" + i + "@absa.africa"));
            thread.start();
        }
    }

    /*
    private static void postMessage(String email, String eventType, int eventSize) throws IOException {
        HttpURLConnection con = getHttpURLConnection();
        String jsonInputString = "{\n" +
                "    \"key\": {\n" +
                "        \"type\": \"STRING\",\n" +
                "        \"data\": \"" + email + "\"\n" +
                "    },\n" +
                "    \"value\": {\n" +
                "        \"type\": \"JSON\",\n" +
                "        \"data\": {\n" +
                "            \"email\": \"" + email + "\",\n" +
                "            \"eventType\": \"" + eventType + "\",\n" +
                "            \"eventSize\": " + eventSize + "\n" +
                "        }\n" +
                "    }\n" +
                "}";
        try(OutputStream os = con.getOutputStream()) {
            byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
        }
    }
     */
    private static HttpURLConnection getHttpURLConnection() throws IOException {
        URL url = new URL("https://pkc-q283m.af-south-1.aws.confluent.cloud/kafka/v3/clusters/lkc-zmowrd/topics/duck_hunt_demo/records");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Authorization", "Basic TExRU0hQRko2TUVGUjJLSToyM0VGdUE2V2h4V3V4cnpNUW9TSjlGcmVHVUo2cmVXKzcxeWk4WE1SQU0yUmlwMzlOa1lTMS9GTWVXbkdsMVUr");
        con.setDoOutput(true);
        return con;
    }

    private static void writeTopic(DuckHuntEvent duckHuntEvent) {

        KafkaProducer<String, DuckHuntEvent> producer = getProducer();
        String key = duckHuntEvent.getEmail().toString();

        ProducerRecord<String, DuckHuntEvent> record = new ProducerRecord<>("duck_hunt_demo", key, duckHuntEvent);
        out.println("send");
        try {
            producer.send(record, (metadata, exception) -> {
                out.println("Callback");
                if (exception != null) exception.printStackTrace();
            });
        } catch (SerializationException e) {
            // may need to do something with it
            e.printStackTrace();
        }
        producer.flush();
    }

    private static final String lock = "Lock";
    private static KafkaProducer<String, DuckHuntEvent> producer = null;
    private static KafkaProducer<String, DuckHuntEvent> getProducer() {
        if (producer != null) return producer;
        synchronized (lock) {
            if (producer == null) {
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-q283m.af-south-1.aws.confluent.cloud:9092");
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        org.apache.kafka.common.serialization.StringSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
                props.put("auto.register.schemas", "false");
                props.put("security.protocol", "SASL_SSL");
                // B3FBQ5SJB4UN7HG4
                // v2A7kQBIU966N1Tl/S5tdZuOrvICNX7p8W/miiv/pNgIfJ/2iCv4CZh6+n0jU0uM
                props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"LLQSHPFJ6MEFR2KI\" password=\"23EFuA6WhxWuxrzMQoSJ9FreGUJ6reW+71yi8XMRAM2Rip39NkYS1/FMeWnGl1U+\";");
                props.put("sasl.mechanism", "PLAIN");
                props.put("client.dns.lookup", "use_all_dns_ips");
                props.put("schema.registry.url", "https://psrc-4v1qj.eu-central-1.aws.confluent.cloud");
                props.put("basic.auth.credentials.source", "USER_INFO");
                props.put("basic.auth.user.info", "B3FBQ5SJB4UN7HG4:v2A7kQBIU966N1Tl/S5tdZuOrvICNX7p8W/miiv/pNgIfJ/2iCv4CZh6+n0jU0uM");
                producer = new KafkaProducer<>(props);
            }
            return producer;
        }
    }


    private String email;
    @Override
    public void run() {
        int score = 0;
        for (int i = 0; i < 100; i++) {
            try {
                Thread.sleep(Math.round(Math.random() * 1000));
                Double rand = Math.random();
                int hit = (rand > 0.9) ? 2 : rand > 0.5 ? 1 : 0;
                DuckHuntEvent value = DuckHuntEvent.newBuilder()
                        .setEmail("email@absa.africa")
                        .setEventSize(1)
                        .setEventType(EventType.SHOT)
                        .build();
                writeTopic(value);
                value = DuckHuntEvent.newBuilder()
                        .setEmail("email@absa.africa")
                        .setEventSize(hit)
                        .setEventType(EventType.HIT)
                        .build();
                writeTopic(value);
                score += 100 * hit;
                value = DuckHuntEvent.newBuilder()
                        .setEmail("email@absa.africa")
                        .setEventSize(score)
                        .setEventType(EventType.SCORE)
                        .build();
                writeTopic(value);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
