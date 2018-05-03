package com.kafkastream.service;

import com.kafkastream.config.StreamsBuilderConfig;
import com.kafkastream.model.Customer;
import com.kafkastream.model.CustomerOrder;
import com.kafkastream.model.Order;
import com.kafkastream.web.kafkarest.StateStoreRestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;


public class EventsListener
{
    private static KafkaStreams streams;

    private static StreamsBuilder  streamsBuilder;

    private static Properties properties;

    private static void setUp()
    {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "cqrs-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG,"localhost:8095");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "2000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", Serdes.String().deserializer().getClass());
        properties.put("value.deserializer", SpecificAvroDeserializer.class);

        //streamsBuilder = StreamsBuilderConfig.getInstance();
        streamsBuilder = new StreamsBuilder();
    }

    public static void main(String[] args)
    {
        setUp();
        List<CustomerOrder> customerOrderList=null;
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
            final HostInfo restEndpoint = new HostInfo("localhost", 8095);
            final StateStoreRestService restService = startRestProxy(streams, restEndpoint);
            customerOrderList=restService.getCustomerOrders();
            printList(customerOrderList);
           /* ReadOnlyKeyValueStore<String, CustomerOrder> customerOrdersStore = waitUntilStoreIsQueryable("customerordersstore", QueryableStoreTypes.keyValueStore(),streams);
            KeyValueIterator<String,CustomerOrder> keyValueIterator=customerOrdersStore.all();
            while(keyValueIterator.hasNext())
            {
                KeyValue<String,CustomerOrder>  customerOrderKeyValue=keyValueIterator.next();
                System.out.println("customerOrderKeyValue.value.toString() ->"+customerOrderKeyValue.value.toString());
            }*/
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

    private static void printList(List<CustomerOrder> customerOrderList)
    {
        for (CustomerOrder customerOrder : customerOrderList)
        {
            System.out.println("customerOrder-> " + customerOrder);
        }
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

    static StateStoreRestService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo) throws Exception
    {
        final StateStoreRestService interactiveQueriesRestService = new StateStoreRestService(streams, hostInfo);
        interactiveQueriesRestService.start();
        return interactiveQueriesRestService;
    }

}
