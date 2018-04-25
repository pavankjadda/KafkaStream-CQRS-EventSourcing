package com.kafkastream;

import com.kafkastream.model.Customer;
import com.kafkastream.model.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TestConsumer
{
    private Properties properties;

    private StreamsBuilder streamsBuilder;

    @Before
    public void setUp()
    {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "cqrs-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", Serdes.String().deserializer().getClass());
        properties.put("value.deserializer", SpecificAvroDeserializer.class);
        streamsBuilder = new StreamsBuilder();
    }

    @Test
    public void consumeCustomerEvent() throws InterruptedException
    {
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        StoreBuilder customerStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("customer-store"),Serdes.String(), customerSerde)
                .withLoggingEnabled(new HashMap<>());

        streamsBuilder.stream("customer", Consumed.with(Serdes.String(), customerSerde)).to("customer-to-ktable-topic",Produced.with(Serdes.String(), customerSerde));
        KTable<String, Customer> customerKTable = streamsBuilder.table("customer-to-ktable-topic", Consumed.with(Serdes.String(), customerSerde),Materialized.as(customerStateStore.name()));
        customerKTable.foreach(((key, value) -> System.out.println("Customer from Topic: " + value)));

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();
            latch.await();
            /*ReadOnlyKeyValueStore<String, Customer> customerStore = streams.store("customer-store", QueryableStoreTypes.keyValueStore());
            System.out.println("customerStore.approximateNumEntries() -> " + customerStore.approximateNumEntries());
        */}

        catch (Exception e)
        {
            e.printStackTrace();
        }

        //Close Runtime
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                latch.countDown();
            }
        });
    }


    @Test
    public void consumeOrderEvent() throws InterruptedException
    {
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        KStream<String, Order> orderKStream = streamsBuilder.stream("order",Consumed.with(Serdes.String(), orderSerde))
                .selectKey((key, value) -> value.getCustomerId().toString());
        orderKStream.to("order-to-ktable-topic",Produced.with(Serdes.String(),orderSerde));
        KTable<String,Order> orderKTable=streamsBuilder.table("order-to-ktable-topic",Consumed.with(Serdes.String(),orderSerde),Materialized.as("order"));
        orderKTable.foreach(((key, value) -> System.out.println("Order from Topic: "+value)));

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();
            latch.await();
            /*ReadOnlyKeyValueStore<String, Customer> customerStore = streams.store("customer-store", QueryableStoreTypes.keyValueStore());
            System.out.println("customerStore.approximateNumEntries() -> " + customerStore.approximateNumEntries());
        */}

        catch (Exception e)
        {
            e.printStackTrace();
        }

        //Close Runtime
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                latch.countDown();
            }
        });
    }



    @Test
    public void queryCustomerStore() throws InterruptedException
    {
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();
            ReadOnlyKeyValueStore<String, Customer> customerStore = waitUntilStoreIsQueryable("customer-store", QueryableStoreTypes.keyValueStore(),streams);
            Customer foundCustomer = customerStore.get("CU1001");
            System.out.println("customerStore.approximateNumEntries()-> " + customerStore.approximateNumEntries());
            latch.await();
            /*ReadOnlyKeyValueStore<String, Customer> customerStore = streams.store("customer-store", QueryableStoreTypes.keyValueStore());
            System.out.println("customerStore.approximateNumEntries() -> " + customerStore.approximateNumEntries());
        */}

        catch (Exception e)
        {
            e.printStackTrace();
        }

        //Close Runtime
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                latch.countDown();
            }
        });

    }


    public static <T> T waitUntilStoreIsQueryable(final String storeName, final QueryableStoreType<T> queryableStoreType, final KafkaStreams streams) throws InterruptedException
    {
        while (true)
        {
            try
            {
                return streams.store(storeName, queryableStoreType);
            }

            catch (InvalidStateStoreException ignored)
            {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }

    private <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {
        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }
}
