package com.kafkastream;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

public class CreateAvroSchema
{
    private final static MediaType SCHEMA_CONTENT = MediaType.parse("application/vnd.schemaregistry.v1+json");
    //private final static String ORDER_SCHEMA ="{\"schema\":\"{\"namespace\": \"com.kafkastream.model\",       \"type\": \"record\",       \"name\": \"Customer\",       \"doc\" : \"RepresentsanEmployeeatacompany\",       \"fields\": [         {\"name\": \"customerId\", \"type\": \"string\", \"doc\": \"Thepersonsgivenname\"},         {\"name\": \"firstName\", \"type\": \"string\", \"doc\": \"Thepersonsgivenname\"},         {\"name\": \"lastName\", \"type\": \"string\", \"doc\": \"Thepersonsgivenname\"},         {\"name\": \"email\", \"type\": \"string\", \"doc\": \"Thepersonsgivenname\"},         {\"name\": \"phone\", \"type\": \"string\", \"doc\": \"Thepersonsgivenname\"}       ]     }\"}";

    private final static String ORDER_SCHEMA = "{\n" +
            "  \"schema\": \"" +
            "  {" +
            "    \\\"namespace\\\": \\\"com.kafkastream.model\\\"," +
            "    \\\"type\\\": \\\"record\\\"," +
            "    \\\"name\\\": \\\"Order\\\"," +
            "    \\\"fields\\\": [" +
            "        {\\\"name\\\": \\\"orderId\\\", \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"customerId\\\", \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"orderItemName\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"orderPlace\\\",  \\\"type\\\": \\\"string\\\"}," +
            "        {\\\"name\\\": \\\"orderPurchaseTime\\\",  \\\"type\\\": \\\"string\\\"}" +
            "    ]" +
            "  }\"" +
            "}";

    public static void main(String[] args)
    {
        try
        {
            OkHttpClient client = new OkHttpClient();

            //POST A NEW SCHEMA
            Request request = new Request.Builder().post(RequestBody.create(SCHEMA_CONTENT, ORDER_SCHEMA)).url("http://localhost:8081/subjects/order-value/versions").build();
            String output = client.newCall(request).execute().body().string();
            System.out.println(output);
        }


        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
