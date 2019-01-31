
#### See for more information about this https://docs.confluent.io/current/schema-registry/docs/using.html


### Valid format for Customer and Customer Orders
curl -X POST -H "Content-Type:application/vnd.schemaregistry.v1+json" --data  '{   "schema":     "{       \"namespace\": \"com.kafkastream.model\",        \"type\": \"record\",        \"name\": \"customer-value\",        \"fields\":          [            {              \"type\": \"string\",              \"name\": \"customerId\"            },            {              \"type\": \"int\",              \"name\": \"firstName\"            },            {              \"type\": \"string\",              \"name\": \"lastName\"            },            {              \"type\": \"string\",              \"name\": \"email\"            },            {              \"type\": \"string\",              \"name\": \"phone\"            }          ]      }" }'  http://localhost:8081/subjects/customer-value/versions

curl -X POST -H "Content-Type:application/vnd.schemaregistry.v1+json" --data  '{   "schema":     "{\"namespace\": \"com.kafkastream.model\",\r\n  \"type\": \"record\",\r\n  \"name\": \"CustomerOrder\",\r\n  \"fields\": [\r\n    {\"name\": \"customerId\", \"type\": \"string\"},\r\n    {\"name\": \"firstName\", \"type\": \"string\"},\r\n    {\"name\": \"lastName\", \"type\": \"string\"},\r\n    {\"name\": \"email\", \"type\": \"string\"},\r\n    {\"name\": \"phone\", \"type\": \"string\"},\r\n    {\"name\": \"orderId\", \"type\": \"string\"},\r\n    {\"name\": \"orderItemName\", \"type\": \"string\"},\r\n    {\"name\": \"orderPlace\", \"type\": \"string\"},\r\n    {\"name\": \"orderPurchaseTime\", \"type\": \"string\"}\r\n  ]\r\n}" }'  http://localhost:8081/subjects/customer-order-value/versions


### Get or Delete schemas
curl -X GET http://localhost:8081/subjects/customer-value/versions/1

curl -X DELETE http://localhost:8081/subjects/customer-value/versions/1


### Following works too
./kafka-avro-console-producer \
         --broker-list localhost:9092 --topic order \
         --property value.schema='{"namespace":"com.kafkastream.model","type":"record","name":"Order","fields":[{"name":"orderId","type":"string"},{"name":"customerId","type":"string"},{"name":"orderItemName","type":"string"},{"name":"orderPlace","type":"string"},{"name":"orderPurchaseTime","type":"string"}]}'


./kafka-avro-console-producer \
                  --broker-list localhost:9092 --topic customer \
                  --property value.schema='{"namespace":"com.kafkastream.model","type":"record","name":"Customer","fields":[{"name":"customerId","type":"string"},{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"email","type":"string"},{"name":"phone","type":"string"}]}'

./kafka-avro-console-producer \
                  --broker-list localhost:9092 --topic customer-order \
                  --property value.schema='{"namespace":"com.kafkastream.model","type":"record","name":"CustomerOrder","fields":[{"name":"customerId","type":"string"},{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"email","type":"string"},{"name":"phone","type":"string"},{"name":"orderId","type":"string"},{"name":"orderItemName","type":"string"},{"name":"orderPlace","type":"string"},{"name":"orderPurchaseTime","type":"string"}]}'
