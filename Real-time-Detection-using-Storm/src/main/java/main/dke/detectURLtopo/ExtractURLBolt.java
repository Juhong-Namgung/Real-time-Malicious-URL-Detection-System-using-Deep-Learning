package main.dke.detectURLtopo;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.kafka.StringScheme;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.esotericsoftware.minlog.Log;

/*
 * Extract URL in Tweet using regular expression(regex)
 */

@SuppressWarnings("serial")
public class ExtractURLBolt extends BaseRichBolt {
    OutputCollector collector;
    private long tuple_size;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        tuple_size = 0l;
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String twitText = (String) input.getValueByField("str");
        String url = null;

        tuple_size += 1;

        // extract URL using regex matching
        url = extractURL(twitText);

        if (!url.equals("")) {
            collector.emit(new Values(twitText, url));
            System.out.println("Extract URL: " + url);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "url"));
    }

    public static String extractURL(String twit) {
        String regex = "(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";

        // pattern matching
        try {
            Pattern patt = Pattern.compile(regex);
            Matcher matcher = patt.matcher(twit);

            if (matcher.find()) {
                return matcher.group();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }
}