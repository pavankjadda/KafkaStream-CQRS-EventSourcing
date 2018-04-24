package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.CustomerOrder;
import com.kafkastream.model.Greetings;
import com.kafkastream.model.Order;
import com.sun.tools.corba.se.idl.constExpr.Or;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class EventsListener
{
    /*@StreamListener
    public void handleGreetings(@Input(GreetingsStreams.INPUT) KStream<String,GreetingsEvent> greetingsEventKStream)
    {
        greetingsEventKStream.foreach((key, value) -> System.out.println("Greetings Message: "+value.getMessage()));
    }*/

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
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<Greetings> greetingsSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<CustomerOrder> customerOrderSerde = createSerde("http://localhost:8081");

        KStream<String, Customer> customerKStream = streamsBuilder.stream("customer",Consumed.with(Serdes.String(), customerSerde));
        customerKStream.foreach(((key, value) -> System.out.println("Customer value from Topic:  " + value.toString())));

        KStream<String, Order> orderKStream = streamsBuilder.stream("order",Consumed.with(Serdes.String(), orderSerde));
        orderKStream.foreach(((key, value) -> System.out.println("Order value from Topic:  " + value.toString())));

        KStream<String, Order> modifiedOrderKStream=orderKStream.selectKey((key, value) -> value.getCustomerId().toString());
        modifiedOrderKStream.foreach(((key, value) -> System.out.println("Modified Key message from Order:  " + key)));

        KStream<String, CustomerOrder> customersOrders = customerKStream.leftJoin(modifiedOrderKStream, (customer, order) ->
        {
            if(customer!=null && order!=null)
            {
                CustomerOrder   customerOrder=new CustomerOrder();
                customerOrder.setCustomerId(customer.getCustomerId());
                customerOrder.setFirstName(customer.getFirstName());
                customerOrder.setLastName(customer.getLastName());
                customerOrder.setEmail(customer.getEmail());
                customerOrder.setPhone(customer.getPhone());
                customerOrder.setOrderId(order.getOrderId());
                customerOrder.setOrderItemName(order.getOrderItemName());
                customerOrder.setOrderPlace(order.getOrderPlace());
                customerOrder.setOrderPurchaseTime(order.getOrderPurchaseTime());

                return customerOrder;
            }
            return null;
        }, JoinWindows.of(TimeUnit.MINUTES.toMillis(5)), Joined.with(Serdes.String(), customerSerde, orderSerde));
        customersOrders.to("customer-orders",Produced.with(Serdes.String(),customerOrderSerde));

        //KTable<String,CustomerOrder> customerOrdersKTable=streamsBuilder.table("customer-orders",Materialized.with(Serdes.String(),customerOrderSerde),);
        KTable<String,CustomerOrder> customerOrdersKTable=streamsBuilder.table("customer-orders",Consumed.with(Serdes.String(),customerOrderSerde),Materialized.as("customer-orders"));
        customerOrdersKTable.foreach(((key, value) -> System.out.println("Customer Orders from Topic: "+value)));


        /*
        KTable<String,Order> customerKTable=streamsBuilder.table("order",Materialized.with(Serdes.String(),orderSerde));
        customerKTable.foreach(((key, value) -> System.out.println("Order from Topic: "+value)));

        KTable<String,Order> orderKTable=streamsBuilder.table("order",Materialized.with(Serdes.String(),orderSerde));
        orderKTable.foreach(((key, value) -> System.out.println("Order from Topic: "+value)));

        KTable<String,String>   customerOrdersKTable=orderKTable.leftJoin(customerKTable,(order,customer)-> order+" and "+customer);
        customerOrdersKTable.foreach(((key, value) -> System.out.println("customerOrders : "+value)));
        */

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
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

        System.exit(0);
    }
    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {

        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }

}
