package com.kafkastream.statestore;

import com.kafkastream.config.StreamsBuilderConfig;
import com.kafkastream.dto.CustomerOrderDTO;
import com.kafkastream.model.CustomerOrder;
import com.kafkastream.web.kafkarest.StateStoreRestService;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.springframework.stereotype.Service;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.*;


/**
 * Not using this class for the moment see EventsListener class for more details
 */

@Service
public class StateStoreService
{

    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private Server jettyServer;

    public StateStoreService()
    {
        /*
        Properties properties = new Properties();
        properties.put("application.id", "cqrs-streams");
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("application.server", "localhost:8096");
        properties.put("commit.interval.ms", "2000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("acks", "all");
        properties.put("key.deserializer", Serdes.String().deserializer().getClass());
        properties.put("value.deserializer", SpecificAvroDeserializer.class); */

        StreamsBuilder streamsBuilder = StreamsBuilderConfig.getInstance();
    }


    public List<CustomerOrderDTO> getCustomerOrders(String customerId)
    {
        List<CustomerOrderDTO> customerOrderDTOList = new ArrayList<>();
        //start();
        //Remote server config. Will replace place holders in future
        String host = "localhost";
        List<CustomerOrder> customerOrderList = client.target(String.format("http://%s:%d/%s", host, 8095, "customer-orders/all")).request(MediaType.APPLICATION_JSON_TYPE).get(new GenericType<List<CustomerOrder>>()
        {
        });

        return convertCustomerOrderListToCustomerOrderDTOList(customerOrderList);
    }

    private List<CustomerOrderDTO> convertCustomerOrderListToCustomerOrderDTOList(List<CustomerOrder> customerOrdersList)
    {
        List<CustomerOrderDTO> customerOrderDTOList = new ArrayList<>();
        for (CustomerOrder customerOrder : customerOrdersList)
        {
            customerOrderDTOList.add(new CustomerOrderDTO(customerOrder.getCustomerId().toString(), customerOrder.getFirstName().toString(), customerOrder.getLastName().toString(), customerOrder.getEmail().toString(), customerOrder.getPhone().toString(), customerOrder.getOrderId().toString(), customerOrder.getOrderItemName().toString(), customerOrder.getOrderPlace().toString(), customerOrder.getOrderPurchaseTime().toString()));
        }
        return customerOrderDTOList;
    }


    private static StateStoreRestService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo) throws Exception
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
                Collection<StreamsMetadata> streamsMetadataCollection = streams.allMetadata();
                for (StreamsMetadata streamsMetadata : streamsMetadataCollection)
                {
                    System.out.println("streamsMetadataIterator.next() -> " + streamsMetadata);
                }
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored)
            {
                // store not yet ready for querying
                Thread.sleep(100);
            }
        }
    }

    private void start() throws Exception
    {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        int port = 8096;
        jettyServer = new Server(port);
        jettyServer.setHandler(context);

        ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();
    }

    void stop() throws Exception
    {
        if (jettyServer != null)
        {
            jettyServer.stop();
        }
    }

    public List<CustomerOrderDTO> getCustomerOrders_Old(String customerId)
    {

        List<CustomerOrderDTO> customerOrderList = new ArrayList<>();
        /*
        Topology topology = streamsBuilder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        CountDownLatch latch = new CountDownLatch(1);
        // This is not part of Runtime.getRuntime() block
        try
        {
            streams.start();
            final HostInfo restEndpoint = new HostInfo("localhost", 8095);
            final StateStoreRestService restService = startRestProxy(streams, restEndpoint);
            customerOrderList = restService.getAllCustomersOrders();
            latch.await();
        } catch (Exception e)
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
        */
        return customerOrderList;
    }


}
