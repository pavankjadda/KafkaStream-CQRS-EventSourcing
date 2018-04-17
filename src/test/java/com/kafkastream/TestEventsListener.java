package com.kafkastream;

import com.kafkastream.config.Schemas;
import com.kafkastream.model.Customer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.net.UnknownHostException;
import java.util.Properties;

public class TestEventsListener
{
    public static void main(String[] args) throws InterruptedException, UnknownHostException
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "cqrs-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.serializer", "com.kafkastream.stream.GenericSerializer");
        properties.put("value.deserializer", "com.kafkastream.stream.GenericDeserializer");
        /*properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
*/
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


        KStream<String, Customer> customerKStream = streamsBuilder.stream("customer", Consumed.with(Serdes.String(), Schemas.Topics.CUSTOMERS.valueSerde()));
        customerKStream.foreach(((key, value) -> System.out.println("Customer key from Topic:  " + key)));

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
            streams.start();
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

}
