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
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class EventsListener
{
    private static KafkaStreams streams;

    private static List<CustomerOrder>  customerOrders=new ArrayList<>();

    private static StreamsBuilder  streamsBuilder;

    private static Properties properties;

    private static void setUp()
    {
        properties = new Properties();
        properties.put("application.id", "cqrs-streams");
        properties.put("application.server", "localhost:8096");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("commit.interval.ms", "2000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", Serdes.String().deserializer().getClass());
        properties.put("value.deserializer", SpecificAvroDeserializer.class);
        streamsBuilder = new StreamsBuilder();
    }

    public static void main(String[] args)
    {
        setUp();
        SpecificAvroSerde<Customer> customerSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<Order> orderSerde = createSerde("http://localhost:8081");
        SpecificAvroSerde<CustomerOrder> customerOrderSerde = createSerde("http://localhost:8081");

        StoreBuilder customerStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("customer-store"),Serdes.String(), customerSerde)
                .withLoggingEnabled(new HashMap<>());
        StoreBuilder orderStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("order-store"),Serdes.String(), customerSerde)
                .withLoggingEnabled(new HashMap<>());
        StoreBuilder customerOrderStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("customerordersstore"),Serdes.String(), customerSerde)
                .withLoggingEnabled(new HashMap<>()).withCachingEnabled();
        KTable<String, Customer> customerKTable = streamsBuilder.table("customer",Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as(customerStateStore.name())
                .withKeySerde(Serdes.String())
                .withValueSerde(customerSerde));
        customerKTable.foreach(((key, value) -> System.out.println("Customer from Topic: " + value)));

        streamsBuilder.stream("order",Consumed.with(Serdes.String(), orderSerde))
                .selectKey((key, value) -> value.getCustomerId().toString()).to("order-to-ktable",Produced.with(Serdes.String(),orderSerde));
        KTable<String,Order> orderKTable=streamsBuilder.table("order-to-ktable",Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as(orderStateStore.name())
                .withKeySerde(Serdes.String())
                .withValueSerde(orderSerde));
        orderKTable.foreach(((key, value) -> System.out.println("Order from Topic: " + value)));

        KTable<String,CustomerOrder> customerOrderKTable=customerKTable.join(orderKTable,(customer, order)->
        {
            if(customer!=null && order!=null)
            {
                CustomerOrder customerOrder=new CustomerOrder();
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
            return  null;
        },Materialized.<String, CustomerOrder, KeyValueStore<Bytes, byte[]>>as(customerOrderStateStore.name()).withKeySerde(Serdes.String()).withValueSerde(customerOrderSerde));
        customerOrderKTable.foreach(((key, value) -> System.out.println("Customer Order -> "+value.toString())));

        Topology topology = streamsBuilder.build();
        streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();
            ReadOnlyKeyValueStore<String, CustomerOrder> customerOrdersStore = waitUntilStoreIsQueryable("customerordersstore", QueryableStoreTypes.keyValueStore(),streams);
            KeyValueIterator<String,CustomerOrder> keyValueIterator=customerOrdersStore.all();
            while(keyValueIterator.hasNext())
            {
                KeyValue<String,CustomerOrder>  customerOrderKeyValue=keyValueIterator.next();
                customerOrders.add(customerOrderKeyValue.value);
                System.out.println("customerOrderKeyValue.value.toString() ->"+customerOrderKeyValue.value.toString());
            }
            latch.await();
        }
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
        System.exit(0);
    }



    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {

        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
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

}
