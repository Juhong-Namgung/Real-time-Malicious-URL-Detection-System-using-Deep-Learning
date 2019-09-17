package main.dke.detectURLtopo;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/*
 * Expand Short URL to Full URL
 */

@SuppressWarnings("serial")
public class ExpandURLBolt extends BaseRichBolt {
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String url = (String) input.getValueByField("url");
        String expandURL;

        expandURL = url;

        if (!expandURL.equals("")) {
            try {
                expandURL = expandURL(url);
                System.out.println(expandURL);
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (!(expandURL == null))
                collector.emit(new Values(input.getValueByField("text"), expandURL));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "expandurl"));
    }

    // Get Full URL Using URLConnection
    public String expandURL(String shortURL) throws IOException {
        URL url = new URL(shortURL);

        URLConnection conn = url.openConnection(Proxy.NO_PROXY);
        ((HttpURLConnection) conn).setInstanceFollowRedirects(false);

        String expandURL = conn.getHeaderField("Location");

        return expandURL;
    }
}