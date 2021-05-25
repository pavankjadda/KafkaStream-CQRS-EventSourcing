package com.pj.cqrs.kafkastream;

import com.pj.cqrs.kafkastream.model.Customer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TestEventsListener
{
    public static void main(String[] args)
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"cqrs-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", SpecificAvroDeserializer.class);
        properties.put("value.deserializer", SpecificAvroDeserializer.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        {
        /*
        KStream<String, String> customerKStream = streamsBuilder.stream("customer", Consumed.with(Serdes.String(), Serdes.String()).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        customerKStream.foreach(((key, value) -> System.out.println("Customer key from Topic:  " + key)));

        KStream<String, String> orderKStream = streamsBuilder.stream("order");
        orderKStream.foreach(((key, value) -> System.out.println("Order key from Topic: " + key)));

        KStream<String, String> orderKStream = streamsBuilder.stream("order");
        orderKStream.foreach(((key, value) -> System.out.println("Order from Topic: " + value)));
        long joinWindowSizeMs = 5L * 60L * 1000L; // 5 minutes
        long windowRetentionTimeMs = 30L * 24L * 60L * 60L * 1000L; // 30 days


        // Java 8
        KStream<String, String> customersOrders = customerKStream.leftJoin(orderKStream,(customer,order)->customer+" "+order,
                JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                Joined.with(
                        Serdes.String(),
                        Serdes.String(),
                        Serdes.String())
        );*/


            // Java 7 example
        /*        KStream<String, String> customersOrders = customerKStream.leftJoin(orderKStream,
                new ValueJoiner<String, String, String>() {
                    @Override
                    public String apply(String leftValue, String rightValue) {
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                Joined.with(
                        Serdes.String(), *//* key *//*
                        Serdes.String(),   *//* left value *//*
                        Serdes.String())  *//* right value *//*
        );


        //customersOrders.foreach(((key, value) -> System.out.println("Order from customersOrders: " + value)));

        KTable<String,String>   customerKTable=streamsBuilder.table("customer",Consumed.with(Topology.AutoOffsetReset.EARLIEST),Materialized.as("customer-store"));
        //System.out.println("Store Name: " + customerKTable.queryableStoreName());
        customerKTable.foreach(((key, value) -> System.out.println("Customer from Topic: " + value)));

        */
        }
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");

        //SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081/");
        KStream<String, Customer> customerKStream = streamsBuilder.stream("customer", Consumed.with(Serdes.String(), customerSerde));
        customerKStream.foreach(((key, value) -> System.out.println("Customer value from Topic:  " + value.toString())));

        /*KTable<String, String> ordersKTable = streamsBuilder.table("order",Materialized.as("OrderKeyStore"));
        ordersKTable.foreach(((key, value) -> System.out.println("Orders KTable from Topic: key-> " + key + " , value-> " + value)));
*/

       /* KStream<String, Order> ordersKTable = streamsBuilder.stream("order");
        ordersKTable=ordersKTable.selectKey((key,value)->value.getCustomerId());
        ordersKTable.foreach(((key, value) -> System.out.println("Order key from Topic:  " + key)));
*/
      /*  Topic<String, Customer> CUSTOMERS=new Topic<>(Serdes.String(),new SpecificAvroSerde<Customer>());
        Topic<String, Order> ORDERS=new Topic<>(Serdes.String(),new SpecificAvroSerde<Order>());
        KStream<String, String> customerOrders=customerKStream.join(ordersKTable,(customer,order)->customer+" created "+ order,JoinWindows.of(TimeUnit.MINUTES.toMillis(5)));

        *//*KStream<String, String> customerOrders = customerKStream.leftJoin(ordersKTable,(customer,order)->customer+" created "+ order,JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                                                                Joined.with(Serdes.String(),CUSTOMERS.keySerde(),ORDERS.valueSerde()));*//*
        customerOrders.foreach(((key, value) -> System.out.println("customerOrders-> Key: "+key+" and Value: "+value)));*/


        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
            }
        });

        {
        /*
        StoreBuilder orderKeyValueStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("OrderKeyValueStore"),Serdes.String(), Serdes.String())
                                           .withLoggingEnabled(new HashMap<>());
        streamsBuilder.addStateStore(orderKeyValueStore);
        // Get the key-value store OrderKeyValueStore
        ReadOnlyKeyValueStore<String, String> keyValueStore =streams.store("OrderKeyValueStore", QueryableStoreTypes.keyValueStore());
        // Get value by key
        System.out.println("keyValueStore.approximateNumEntries(): "+keyValueStore.approximateNumEntries());
        System.out.println("Order Value:" + keyValueStore.get("ORD1001"));

      Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                latch.countDown();
            }
        });


        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.startJettyRestProxyServer();
            latch.await();

        } catch (Throwable e)
        {
            System.exit(1);
        }

        System.exit(0);*/
        }
    }

    public static class Topic<K, V>
    {

        private Serde<K> keySerde;
        private Serde<V> valueSerde;

        Topic(Serde<K> keySerde, Serde<V> valueSerde)
        {
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        public Serde<K> keySerde()
        {
            return keySerde;
        }

        public Serde<V> valueSerde()
        {
            return valueSerde;
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
