package com.kafkastream.jersey;


import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class JettyRESTServer
{
    private Server jettyServer;

    @BeforeAll
    void setUp()
    {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(8100);
        jettyServer.setHandler(context);

        ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);
        jerseyServlet.setInitParameter("jersey.config.server.provider.classnames", Calculator.class.getCanonicalName());

    }

    @Test
    void startServer()
    {
        //new JHades().overlappingJarsReport();
        try
        {
            jettyServer.start();
            jettyServer.join();
        }

        catch (Exception e)
        {
            e.printStackTrace();
        }

    }

    @AfterAll
    void closeJettyServer()
    {
        jettyServer.destroy();
    }

}
