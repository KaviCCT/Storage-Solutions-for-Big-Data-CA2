package com.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class WordCountTopology {
    public static class TextFileSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private BufferedReader reader;
        private String line;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
            try {
                reader = new BufferedReader(new FileReader("input/A_Tale_of_Two_Cities.txt"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void nextTuple() {
            try {
                if ((line = reader.readLine()) != null) {
                    collector.emit(new Values(line));
                } else {
                    Utils.sleep(100); // Sleep if no more lines to read
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    public static class WordSplitBolt extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String line = tuple.getStringByField("line");
            String[] words = line.split("\\s+");
            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseBasicBolt {
        private Map<String, Integer> wordCounts = new HashMap<>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getStringByField("word");
            int count = wordCounts.getOrDefault(word, 0) + 1;
            wordCounts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("text-file-spout", new TextFileSpout(), 1);
        builder.setBolt("word-split-bolt", new WordSplitBolt(), 2).shuffleGrouping("text-file-spout");
        builder.setBolt("word-count-bolt", new WordCountBolt(), 2).fieldsGrouping("word-split-bolt", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count-topology", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}
