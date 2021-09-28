package com.example.demo;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import org.apache.camel.component.jms.JmsComponent;

import javax.jms.ConnectionFactory;
import javax.sql.DataSource;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class MyRoute extends RouteBuilder {

    @Autowired
    CamelContext camel;

    @Autowired
    DataSource dataSource;

    @Autowired
    JmsTemplate jms;

    @Override
    public void configure() throws Exception {
        AtomicInteger txt = new AtomicInteger(0);
        AtomicInteger xml = new AtomicInteger(0);
        AtomicInteger another = new AtomicInteger(0);

        camel.getPropertiesComponent().setLocation("classpath:application.properties");
        dataSource = new DriverManagerDataSource("jdbc:mysql://localhost:3306/schema_for_apache?user=root&password=***");
        camel.getRegistry().bind("db", dataSource);
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        camel.getRegistry().bind("jms", JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));

        from("file:{{from}}")
                .choice()

                .when(exchange -> {
                    String fileName = (exchange.getIn().getBody(File.class)).getName();
                    if (fileName.endsWith(".xml")) {
                        xml.incrementAndGet();
                        return true;
                    } else return false;
                })
//                .to("file:{{xml}}")
                .convertBodyTo(String.class, "UTF-8") //кодировка для кириллицы
                .to("jms:queue:test_queue")

                .when(exchange -> {
                    String fileName = (exchange.getIn().getBody(File.class)).getName();
                    if (fileName.endsWith(".txt")) {
                        txt.incrementAndGet();
                        return true;
                    } else return false;
                })
                .to("file:{{txt}}")
                .to("jms:queue:test_queue")
                .process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        Message in = exchange.getIn();
                        String fileName = (in.getBody(File.class)).getName(); //получаем название файла
                        String body = in.getBody(String.class); //получаем содержимое файла
                        exchange.getIn().setBody(String.format("insert into files (`file_name`, `file_body`) values ('%s', '%s');", fileName, body));
                    }
                })
                .to("jdbc:db")
//                .otherwise()
//                .process(new Processor() {
//                    @Override
//                    public void process(Exchange exchange) throws Exception {
//                        another.incrementAndGet();
//                    }
//                })
                .when(exchange -> {
                    String fileName = (exchange.getIn().getBody(File.class)).getName();
                    if (!fileName.endsWith(".xml") || !fileName.endsWith(".txt")) {
                        another.incrementAndGet();
                        return true;
                    }
                    else return false;
                })
                .convertBodyTo(String.class, "UTF-8")
                .to("jms:queue:invalid_queue")

                .when(exchange -> {
                    if ((txt.get() % 100) == 0 || (xml.get() % 100) == 0 || (another.get() % 100) == 0) {
                        return true;
                    }
                    return false;
                }).to("smtps://smtp.yandex.ru:465?username=***&password=***&to=***@domen.ru");
    }
}
