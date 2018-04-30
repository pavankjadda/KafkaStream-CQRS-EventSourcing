package com.kafkastream.service;

import com.kafkastream.model.Customer;
import com.kafkastream.model.CustomerOrder;
import com.kafkastream.model.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.*;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CountDownLatch;

@Service
public class StateStoreService
{
    private Properties properties;

    private StreamsBuilder streamsBuilder;

    public StateStoreService()
    {
        this.properties = new Properties();
        properties.put("application.id", "cqrs-streams");
        properties.put("application.server", "localhost:8095");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("auto.offset.reset", "earliest");
        properties.put("topic.metadata.refresh.interval.ms","100");
        properties.put("commit.interval.ms", "100");
        properties.put("acks", "all");
        properties.put("key.deserializer", Serdes.String().deserializer().getClass());
        properties.put("value.deserializer", SpecificAvroDeserializer.class);

        this.streamsBuilder = new StreamsBuilder();
    }

    public List<CustomerOrder> getCustomerOrders(String customerId) throws InterruptedException
    {

        List<CustomerOrder> customerOrderList = new ArrayList<>();

        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();
            ReadOnlyKeyValueStore<String, CustomerOrder> customerOrdersStore = waitUntilStoreIsQueryable("customer-orders-store", QueryableStoreTypes.keyValueStore(),streams);
            KeyValueIterator<String,CustomerOrder> keyValueIterator=customerOrdersStore.all();
            while(keyValueIterator.hasNext())
            {
                KeyValue<String,CustomerOrder>  customerOrderKeyValue=keyValueIterator.next();
                customerOrderList.add(customerOrderKeyValue.value);
                System.out.println("customerOrderKeyValue.value.toString() ->"+customerOrderKeyValue.value.toString());
            }
            latch.await();
            //System.out.println("customerOrdersStore.approximateNumEntries() -> " + customerOrdersStore.approximateNumEntries());
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

        return customerOrderList;
    }

    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(final String schemaRegistryUrl)
    {

        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        serde.configure(serdeConfig, false);
        return serde;
    }

    private static <T> T waitUntilStoreIsQueryable(final String storeName, final QueryableStoreType<T> queryableStoreType, final KafkaStreams streams) throws InterruptedException
    {
        while (true)
        {
            try
            {
                Collection<StreamsMetadata> streamsMetadataList=streams.allMetadata();
                //Collection<StreamsMetadata> streamsMetadataList=streams.allMetadataForStore(storeName);
                System.out.println("streamsMetadataList.size(): -> "+streamsMetadataList.size());
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored)
            {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }



}
