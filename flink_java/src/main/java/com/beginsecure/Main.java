package com.beginsecure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class Main {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String[] featureNames = new String[] {
            "id", "income", "name_email_similarity", "prev_address_months_count",
            "current_address_months_count", "customer_age", "days_since_request", "intended_balcon_amount",
            "zip_count_4w", "velocity_6h", "velocity_24h", "velocity_4w", "bank_branch_count_8w",
            "date_of_birth_distinct_emails_4w", "credit_risk_score", "email_is_free", "phone_home_valid",
            "phone_mobile_valid", "bank_months_count", "has_other_cards", "proposed_credit_limit",
            "foreign_request", "session_length_in_minutes", "keep_alive_session",
            "device_distinct_emails_8w", "device_fraud_count", "month"
    };

    private static final String COEFF_JSON_PATH = "/home/javie/lr_model2.json";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "34.52.238.189:9092";
    private static final String KAFKA_INPUT_TOPIC = "debezium.public.transactions";
    private static final String KAFKA_OUTPUT_TOPIC = "resultados_inferencia";

    public static void main(String[] args) throws Exception {
        // Entorno Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configuración Kafka
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty("group.id", "flink-inference-group");

        // Consumer de Kafka (datos de entrada)
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                KAFKA_INPUT_TOPIC,
                new SimpleStringSchema(),
                props
        );

        System.out.println("Kafka consumer creado. Servidor: " + KAFKA_BOOTSTRAP_SERVERS + ", topic: " + KAFKA_INPUT_TOPIC);

        // Producer de Kafka (resultados de inferencia)
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                KAFKA_OUTPUT_TOPIC,
                new SimpleStringSchema(),
                props
        );

        // Pipeline: Kafka → Inference → Kafka
        env.addSource(consumer)
                .map(new InferenceFunction(COEFF_JSON_PATH))
                .addSink(producer);

        env.execute("Flink Logistic Regression Inference");
    }

    // --- Función de inferencia ---
    public static class InferenceFunction extends RichMapFunction<String, String> {

        private final String modelPath;
        private double[] coefficients;
        private double intercept;

        public InferenceFunction(String modelPath) {
            this.modelPath = modelPath;
        }

        @Override
        public void open(Configuration parameters) throws IOException {
            JsonNode root = mapper.readTree(new File(modelPath));
            JsonNode coefNode = root.get("coefficients");
            intercept = root.get("intercept").asDouble();

            coefficients = new double[coefNode.size()];
            for (int i = 0; i < coefNode.size(); i++) {
                coefficients[i] = coefNode.get(i).asDouble();
            }

            System.out.println("Modelo cargado: " + coefficients.length + " coeficientes, intercept = " + intercept);
        }

        @Override
        public String map(String msg) {
            System.out.println("Mensaje recibido: " + msg);

            try {
                JsonNode parsed = mapper.readTree(msg);
                JsonNode payload = parsed.get("payload");

                if (payload == null) {
                    throw new RuntimeException("No 'payload' field found");
                }

                // Acceder a 'after' dentro de payload
                JsonNode after = payload.get("after");
                if (after == null) {
                    throw new RuntimeException("No 'after' field found inside payload");
                }

                double score = 0.0;
                String idValue = null;

                for (int i = 0; i < featureNames.length; i++) {
                    String feature = featureNames[i];
                    JsonNode valNode = after.get(feature);

                    if (valNode == null) {
                        throw new RuntimeException("Missing feature: " + feature + " in 'after'");
                    }

                    if ("id".equals(feature)) {
                        // Guardamos id, pero no lo usamos para score
                        idValue = valNode.asText();
                        continue; // saltamos la multiplicación con coeficiente
                    }

                    double val;
                    if (valNode.isNumber()) {
                        val = valNode.asDouble();
                    } else if (valNode.isTextual()) {
                        val = valNode.asText().hashCode() % 1000 * 1.0;
                    } else {
                        throw new RuntimeException("Unsupported JSON node type for feature: " + feature);
                    }

                    if (i - 1 >= coefficients.length) { // -1 porque no usamos id
                        throw new RuntimeException("Coefficient index out of bounds: " + (i-1));
                    }

                    score += coefficients[i - 1] * val; // ajustamos índice de coeficiente
                }

                score += intercept;
                double probability = 1.0 / (1.0 + Math.exp(-score));

                // Copiar todas las columnas de 'after' y añadir score/probability
                ObjectNode enrichedPayload = after.deepCopy();
                enrichedPayload.put("score", score);
                enrichedPayload.put("probability", probability);

                return mapper.writeValueAsString(enrichedPayload);

            } catch (Exception e) {
                return "{\"error\": \"" + e.getMessage() + "\"}";
            }
        }
    }
}
