package com.kafkastream;

import com.kafkastream.model.Customer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class TestProducer
{
    public static void main(String[] args)
    {
        try
        {
            // When configuring the default serdes of StreamConfig
            Properties properties = new Properties();
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, "cqrs-streams");
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class.getName());

            // When you want to override serdes explicitly/selectively
            SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
            Producer<String,Customer>   producer=new KafkaProducer<>(properties,Serdes.String().serializer(),customerSerde.serializer());

            Customer customer = new Customer();
            customer.setCustomerId("CU1001");
            customer.setFirstName("John");
            customer.setLastName("Doe");
            customer.setEmail("john.doe@gmail.com");
            customer.setPhone("993-332-9832");

            ProducerRecord<String, Customer> customerRecord = new ProducerRecord<>("customer",customer.getCustomerId(), customer);
            Future<RecordMetadata> future = producer.send(customerRecord);
            System.out.println("Customer record sent. Customer Id: " + customer.getCustomerId());
            System.out.println("Customer future.get(): " + future.get());


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {
        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }

}
