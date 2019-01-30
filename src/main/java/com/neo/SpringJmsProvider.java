package com.neo;

import org.apache.storm.jms.JmsProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;

/**
 * @author wf
 * @Description SpringJmsProvider
 * @Date 2019/1/29 17:13
 */
public class SpringJmsProvider implements JmsProvider {
    private ConnectionFactory connectionFactory;
    private Destination destination;

    /**
     * 用参数构造一个 <code>SpringJmsProvider</code> 对象
     *
     * @param appContextClasspathResource - springContext.xml
     * @param connectionFactoryBean       - the JMS connection factory bean 名称
     * @param destinationBean             - the JMS destination bean 名称
     */
    SpringJmsProvider(String appContextClasspathResource, String connectionFactoryBean, String destinationBean) {
        ApplicationContext context = new ClassPathXmlApplicationContext(appContextClasspathResource);
        this.connectionFactory = (ConnectionFactory) context.getBean(connectionFactoryBean);
        this.destination = (Destination) context.getBean(destinationBean);
    }

    public ConnectionFactory connectionFactory() {
        return this.connectionFactory;
    }

    public Destination destination() {
        return this.destination;
    }
}
