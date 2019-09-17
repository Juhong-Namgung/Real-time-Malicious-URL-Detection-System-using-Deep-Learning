package main.dke.detectURLtopo;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ValidateURLBolt extends BaseRichBolt {
    OutputCollector collector;
    private long tuple_size;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        tuple_size = 0l;
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String expandURL = (String) input.getValueByField("expandurl");
        String twitText = (String) input.getValueByField("text");
        tuple_size += twitText.getBytes().length;

        if (!expandURL.equals("")) {
            // Get HTTP Status Code
            try {
                URL url = new URL(expandURL);
                HttpURLConnection http = (HttpURLConnection) url.openConnection();
                int statusCode = http.getResponseCode();

                if (statusCode >= 400) {
                    System.out.println("#######URL: " + expandURL + " is not valid!! HTTP Status Code: " + statusCode);
                } else {
                    collector.emit(new Values(input.getValueByField("text"), expandURL));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "validurl"));
    }
}