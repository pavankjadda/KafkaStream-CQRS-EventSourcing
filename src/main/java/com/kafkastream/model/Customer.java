package com.kafkastream.model;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificRecord;


import java.util.Objects;

public class Customer  extends org.apache.avro.specific.SpecificRecordBase implements SpecificRecord
{
    public String   customerId;

    public String   firstName;

    public String   lastName;

    public String   email;

    public String   phone;

    private static final long serialVersionUID = -6112285611684054927L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\n" + "      \"type\": \"record\",\n" + "      \"name\": \"Customer\",\n" + "      \"namespace\": \"com.kafkastream.model\",\n" + "      \"fields\": [\n" + "        {\"name\": \"customerId\", \"type\": \"string\"},\n" + "        {\"name\": \"firstName\", \"type\": \"string\"},\n" + "        {\"name\": \"lastName\", \"type\": \"string\"},\n" + "        {\"name\": \"email\", \"type\": \"string\"},\n" + "        {\"name\": \"phone\", \"type\": \"string\"}\n" + "      ]\n" + "}\n");


    public Customer()
    {

    }
    public Customer(String customerId, String firstName, String lastName, String email, String phone)
    {
        this.customerId = customerId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.phone = phone;
    }

    public String getCustomerId()
    {
        return customerId;
    }

    public void setCustomerId(String customerId)
    {
        this.customerId = customerId;
    }

    public String getFirstName()
    {
        return firstName;
    }

    public void setFirstName(String firstName)
    {
        this.firstName = firstName;
    }

    public String getLastName()
    {
        return lastName;
    }

    public void setLastName(String lastName)
    {
        this.lastName = lastName;
    }

    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }

    public String getPhone()
    {
        return phone;
    }

    public void setPhone(String phone)
    {
        this.phone = phone;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Customer customer = (Customer) o;
        return Objects.equals(customerId, customer.customerId) && Objects.equals(firstName, customer.firstName) && Objects.equals(lastName, customer.lastName) && Objects.equals(email, customer.email) && Objects.equals(phone, customer.phone);
    }

    @Override
    public int hashCode()
    {

        return Objects.hash(customerId, firstName, lastName, email, phone);
    }

    @Override
    public String toString()
    {
        return "Customer{" + "customerId='" + customerId + '\'' + ", firstName='" + firstName + '\'' + ", lastName='" + lastName + '\'' + ", email='" + email + '\'' + ", phone='" + phone + '\'' + '}';
    }


    /**
     * Set the value of a field given its position in the schema.
     * <p>This method is not meant to be called by user code, but only by {@link
     * DatumReader} implementations.
     *
     * @param i
     * @param v
     */
    @Override
    public void put(int i, Object v)
    {
        this.put(i,v);
    }

    /**
     * Return the value of a field given its position in the schema.
     * <p>This method is not meant to be called by user code, but only by {@link
     * DatumWriter} implementations.
     *
     * @param i
     */
    @Override
    public Object get(int i)
    {
        return this.toString();
    }

    /**
     * The schema of this instance.
     */
    @Override
    public Schema getSchema()
    {

        return SCHEMA$;
    }
}
