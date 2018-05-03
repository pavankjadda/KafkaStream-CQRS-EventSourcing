package com.kafkastream.service;

import com.kafkastream.config.StreamsBuilderConfig;
import com.kafkastream.model.CustomerOrder;
import com.kafkastream.web.kafkarest.StateStoreRestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
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
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("application.server","localhost:8096");
        properties.put("commit.interval.ms", "2000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", Serdes.String().deserializer().getClass());
        properties.put("value.deserializer", SpecificAvroDeserializer.class);

        this.streamsBuilder = StreamsBuilderConfig.getInstance();
    }

    public List<CustomerOrder> getCustomerOrders(String customerId) throws InterruptedException
    {

        List<CustomerOrder> customerOrderList = new ArrayList<>();

        Topology topology = streamsBuilder.build();
        KafkaStreams streams=new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();
            final HostInfo restEndpoint = new HostInfo("localhost", 8095);
            final StateStoreRestService restService = startRestProxy(streams, restEndpoint);
            customerOrderList=restService.getCustomerOrders();
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

        return customerOrderList;
    }

    static StateStoreRestService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo) throws Exception
    {
        final StateStoreRestService interactiveQueriesRestService = new StateStoreRestService(streams, hostInfo);
        interactiveQueriesRestService.start();
        return interactiveQueriesRestService;
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
                Collection<StreamsMetadata> streamsMetadataCollection=streams.allMetadata();
                Iterator<StreamsMetadata> streamsMetadataIterator=streamsMetadataCollection.iterator();
                while (streamsMetadataIterator.hasNext())
                {
                    System.out.println("streamsMetadataIterator.next() -> "+streamsMetadataIterator.next());
                }
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored)
            {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }

}
