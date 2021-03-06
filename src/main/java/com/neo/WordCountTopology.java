package com.neo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import javax.jms.Session;

/**
 * @author wf
 * @Description WordCountTopology
 * @Date 2019/1/30 9:22
 */
public class WordCountTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // JMS Queue Provider
        JmsProvider jmsQueueProvider = new SpringJmsProvider("jms-activemq.xml", "jmsConnectionFactory",
                "notificationQueue");
        //JMS Queue Provider,用来发送结果到MQ
        JmsProvider sendProvider = new SpringJmsProvider("jms-activemq.xml", "jmsConnectionFactory",
                "sendQueue");

        // JMS Producer
        JmsTupleProducer producer = new MyJmsTupleProducer();

        //Jms Queue Spout
        JmsSpout queueSpout = new JmsSpout();
        queueSpout.setJmsProvider(jmsQueueProvider);
        queueSpout.setJmsTupleProducer(producer);
        queueSpout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);

        //Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("jmsSpout", queueSpout, 1);
        builder.setBolt("splitBolt", new SplitSentenceBolt(), 1).shuffleGrouping("jmsSpout");
        builder.setBolt("countBolt", new WordCountBolt(), 1)
                .fieldsGrouping("splitBolt", new Fields("word"))
                .shuffleGrouping("jmsSpout");
        //新增sendBolt，用来把结果发送到ActiveMQ
        JmsBolt sendBolt = new JmsBolt();
        sendBolt.setJmsProvider(sendProvider);
        sendBolt.setJmsMessageProducer((JmsMessageProducer) (session, input) -> {
            System.out.println("发送结果到MQ:" + input.toString());
            return session.createTextMessage(input.getStringByField("countResult"));
        });
        builder.setBolt("sendBolt", sendBolt, 1).shuffleGrouping("countBolt");
        //新增MongoCountRecordBolt，将查询记录和结果存到MongoDB
        MongoMapper mongoMapper = new SimpleMongoMapper().withFields("sentence", "countResult", "date");
        MongoInsertBolt insertBolt = new MongoInsertBolt("mongodb://127.0.0.1:27017/test",
                "CountRecord", mongoMapper);
        builder.setBolt("mongoCountRecordBolt", insertBolt, 1).shuffleGrouping("countBolt");

        //Topology Config
        Config config = new Config();
        config.setDebug(false);

        if (args != null && args.length > 0) {
            //集群模式
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            //本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-jms-example", config, builder.createTopology());
            //300s后 shutdown
            Utils.sleep(300000);
            cluster.killTopology("storm-jms-example");
            cluster.shutdown();
        }


    }

}
