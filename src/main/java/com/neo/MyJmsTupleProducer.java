package com.neo;

import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * @author wf
 * @Description MyTupleProducer
 * @Date 2019/1/30 9:08
 */
public class MyJmsTupleProducer implements JmsTupleProducer {

    public Values toTuple(Message msg) throws JMSException {
        String text = ((TextMessage) msg).getText();
        System.out.println("接收到的消息====>"+text);
        return new Values(text);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
