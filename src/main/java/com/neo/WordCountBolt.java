package com.neo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wf
 * @Description WordCountBolt
 * @Date 2019/1/30 9:16
 */
public class WordCountBolt extends BaseRichBolt {

    private Map<String, Long> counts;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        /*因为topology要长时间启动，所以不能在cleanup中打印结果。
         * 所以发送的Message后加EOF来判断,e.g. : Enter some text here for the message body for the message body EOF*/
        if ("EOF".equals(word)) {
            System.out.println("词频统计结果:" + this.counts);
            this.counts.clear();
            return;
        }
        Long count = this.counts.get(word);
        if (count == null) {
            count = 0L;
        }
        count++;
        this.counts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("--------------------cleanup---------------------");
    }
}
