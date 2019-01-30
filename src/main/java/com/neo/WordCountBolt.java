package com.neo;

import com.alibaba.fastjson.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wf
 * @Description WordCountBolt
 * @Date 2019/1/30 9:16
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Long> counts;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        /*因为topology要长时间启动，所以不能在cleanup中打印结果。
         * 所以发送的Message后加EOF来判断,e.g. : Enter some text here for the message body for the message body EOF*/
        if ("EOF".equals(word)) {
            String jsonString = JSON.toJSONString(this.counts);
            System.out.println("词频统计结果:" + jsonString);
            this.collector.emit(new Values(jsonString));
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
        declarer.declare(new Fields("countResult"));
    }

    @Override
    public void cleanup() {
        System.out.println("--------------------cleanup---------------------");
    }
}
