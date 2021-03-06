package com.pj.cqrs.kafkastream;

import com.pj.cqrs.kafkastream.constants.KafkaConstants;
import com.pj.cqrs.kafkastream.dto.CustomerDTO;
import com.pj.cqrs.kafkastream.model.Customer;
import com.pj.cqrs.kafkastream.model.CustomerOrder;
import com.pj.cqrs.kafkastream.model.Greetings;
import com.pj.cqrs.kafkastream.model.Order;
import com.pj.cqrs.kafkastream.web.kafkarest.StateStoreRestService;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/*
    This class starts Jersey REST API server that will be used to get Kafka Materialized view data from State stores
 */
public class EventsListener
{
	private static final Logger logger = LoggerFactory.getLogger(EventsListener.class);
	private static KafkaStreams streams;
	private static StreamsBuilder streamsBuilder;
	private static Properties properties;

	public static void main(String[] args)
	{
		//Setup StreamsBuilder
		setUp();

		//Instantiate Serializer and Deserializers
		SpecificAvroSerde<Customer> customerSerde = createSerde();
		SpecificAvroSerde<Order> orderSerde = createSerde();
		SpecificAvroSerde<Greetings> greetingsSerde = createSerde();
		SpecificAvroSerde<CustomerOrder> customerOrderSerde = createSerde();

		//Define Materialized view state stores
		StoreBuilder<KeyValueStore<String, Customer>> customerStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(KafkaConstants.CUSTOMER_STORE_NAME), Serdes.String(), customerSerde).withLoggingEnabled(new HashMap<>());
		StoreBuilder<KeyValueStore<String, Order>> orderStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(KafkaConstants.ORDER_STORE_NAME), Serdes.String(), orderSerde).withLoggingEnabled(new HashMap<>());
		StoreBuilder<KeyValueStore<String, Greetings>> greetingsStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(KafkaConstants.GREETING_STORE_NAME), Serdes.String(), greetingsSerde).withLoggingEnabled(new HashMap<>());
		StoreBuilder<KeyValueStore<String, CustomerOrder>> customerOrderStateStore = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(KafkaConstants.CUSTOMER_ORDER_STORE_NAME), Serdes.String(), customerOrderSerde).withLoggingEnabled(new HashMap<>()).withCachingEnabled();

		//Create customerKTable
		streamsBuilder.table(KafkaConstants.CUSTOMER_TOPIC, Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as(customerStateStore.name())
				.withKeySerde(Serdes.String())
				.withValueSerde(customerSerde));
		//Create orderKTable
		KTable<String, Order> orderKTable = streamsBuilder.table(KafkaConstants.ORDER_TOPIC, Materialized.<String, Order, KeyValueStore<Bytes, byte[]>>as(orderStateStore.name())
				.withKeySerde(Serdes.String())
				.withValueSerde(orderSerde));
		//Create greetingsKTable
		streamsBuilder.table(KafkaConstants.GREETINGS_TOPIC, Materialized.<String, Greetings,
				KeyValueStore<Bytes, byte[]>>as(greetingsStateStore.name())
				.withKeySerde(Serdes.String())
				.withValueSerde(greetingsSerde));

		//Map orderKTable values and stream them to CUSTOMER_ORDER_TOPIC topic
		orderKTable.mapValues(order ->
		{
			logger.info("orderKTable.value: {}", order);
			CustomerOrder customerOrder = new CustomerOrder();
			customerOrder.setCustomerId(order.getCustomerId());
			CustomerDTO customer = getCustomerInformation(order.getCustomerId());
			if (customer == null)
			{
				customerOrder.setFirstName("");
				customerOrder.setLastName("");
				customerOrder.setEmail("");
				customerOrder.setPhone("");
			}
			else
			{
				customerOrder.setFirstName(customer.getFirstName());
				customerOrder.setLastName(customer.getLastName());
				customerOrder.setEmail(customer.getEmail());
				customerOrder.setPhone(customer.getPhone());
			}

			customerOrder.setOrderId(order.getOrderId());
			customerOrder.setOrderItemName(order.getOrderItemName());
			customerOrder.setOrderPlace(order.getOrderPlace());
			customerOrder.setOrderPurchaseTime(order.getOrderPurchaseTime());
			return customerOrder;
		}).toStream().to(KafkaConstants.CUSTOMER_ORDER_TOPIC, Produced.with(Serdes.String(), customerOrderSerde));

		//Stream data from CUSTOMER_ORDER_TOPIC and build Customer Orders KTable
		streamsBuilder.table(KafkaConstants.CUSTOMER_ORDER_TOPIC, Materialized.<String, CustomerOrder, KeyValueStore<Bytes, byte[]>>as(customerOrderStateStore.name())
				.withKeySerde(Serdes.String())
				.withValueSerde(customerOrderSerde));

		//Start the topology
		Topology topology = streamsBuilder.build();
		streams = new KafkaStreams(topology, properties);
		CountDownLatch latch = new CountDownLatch(1);
		// This is not part of Runtime.getRuntime() block
		try
		{
			//Start streams
			streams.start();
			Thread.sleep(1000L);

			//Define REST proxy server HostInfo and end point
			HostInfo restEndpoint = new HostInfo(KafkaConstants.REST_PROXY_HOST, KafkaConstants.REST_PROXY_PORT);

			//Start State store REST service
			startRestProxy(streams, restEndpoint);

			latch.await();
		}
		catch (Exception e)
		{
			logger.error("Exception occurred while starting REST proxy service. Error: ", e);
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

	//Setup the StreamsBuilder properties
	private static void setUp()
	{
		properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID_CONFIG);
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS_CONFIG);
		properties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, KafkaConstants.APPLICATION_SERVER_CONFIG);
		properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, KafkaConstants.COMMIT_INTERVAL_MS_CONFIG);
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConstants.AUTO_OFFSET_RESET_CONFIG);
		properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
		properties.put("schema.registry.url", KafkaConstants.SCHEMA_REGISTRY_URL);
		properties.put("acks", "all");
		properties.put("key.deserializer", Serdes.String().deserializer().getClass());
		properties.put("value.deserializer", SpecificAvroDeserializer.class);

		streamsBuilder = new StreamsBuilder();
	}

	/* Start REST proxy server using Jetty */
	private static void startRestProxy(final KafkaStreams streams, final HostInfo hostInfo)
	{
		StateStoreRestService stateStoreRestService = new StateStoreRestService(streams, hostInfo);
		stateStoreRestService.startJettyRestProxyServer();
	}

	//Get Customer information from Jetty REST API
	private static CustomerDTO getCustomerInformation(CharSequence customerId)
	{
		RestTemplate restTemplate = new RestTemplate();
		if (customerId != null)
		{
			ResponseEntity<CustomerDTO> customerResponseEntity = restTemplate.exchange(KafkaConstants.REST_PROXY_HOST + ":" + KafkaConstants.REST_PROXY_PORT + "store/customer/" + customerId, HttpMethod.GET, null, CustomerDTO.class);
			return customerResponseEntity.getBody();
		}
		return null;
	}

	//Create Serializer and Deserializer
	private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde()
	{
		SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>();
		Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMA_REGISTRY_URL);
		serde.configure(serdeConfig, false);
		return serde;
	}
}
