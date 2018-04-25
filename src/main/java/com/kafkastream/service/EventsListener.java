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
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class EventsListener
{

    public static void main(String[] args)
    {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"cqrs-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("commit.interval.ms","1000");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", SpecificAvroDeserializer.class);
        properties.put("value.deserializer", SpecificAvroDeserializer.class);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<Greetings> greetingsSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<CustomerOrder> customerOrderSerde = createSerde("http://localhost:8081");

        StoreBuilder customerStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("customer-store"),Serdes.String(), customerSerde)
                                           .withLoggingEnabled(new HashMap<>());
        //streamsBuilder.addStateStore(customerStateStore);

        KTable<String,Customer> customerKTable=streamsBuilder.table("customer",Consumed.with(Serdes.String(),customerSerde),Materialized.as(customerStateStore.name()));
        customerKTable.foreach(((key, value) -> System.out.println("Customer from Topic: "+value)));


        /*KStream<String, Order> orderKStream = streamsBuilder.stream("order",Consumed.with(Serdes.String(), orderSerde))
                                                            .selectKey((key, value) -> value.getCustomerId().toString());
        orderKStream.to("order-to-ktable-topic",Produced.with(Serdes.String(),orderSerde));
        KTable<String,Order> orderKTable=streamsBuilder.table("order-to-ktable-topic",Consumed.with(Serdes.String(),orderSerde),Materialized.as("order"));
        orderKTable.foreach(((key, value) -> System.out.println("Order from Topic: "+value)));

        KTable<String,CustomerOrder>    customerOrderKTable=customerKTable.leftJoin(orderKTable,(customer,order)->
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
        });

        //},Materialized.<String, CustomerOrder, KeyValueStore<Bytes, byte[]>>as("customer-orders"));
        customerOrderKTable.foreach(((key, value) -> System.out.println("Customer Orders from Topic: "+value)));*/

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        //streams.start();

        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();

            while(!streams.state().isRunning())
            {
                Thread.sleep(10000);
                ReadOnlyKeyValueStore<String, Customer> customerStore = streams.store("customer-store", QueryableStoreTypes.keyValueStore());
                Customer foundCustomer = customerStore.get("CU559242116");
                System.out.println("customerStore.approximateNumEntries()-> " + customerStore.approximateNumEntries());

            }



            //latch.await();
        }

        catch (Exception e)
        {
            e.printStackTrace();
        }


        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook")
        {
            @Override
            public void run()
            {
                streams.close();
                //latch.countDown();
            }
        });


        //System.exit(0);
    }
    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {

        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }

}
