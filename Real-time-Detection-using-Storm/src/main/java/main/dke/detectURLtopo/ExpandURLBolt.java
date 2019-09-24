package main.dke.detectURLtopo;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    private static Log LOG = LogFactory.getLog(ExpandURLBolt.class);
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String url = (String) input.getValueByField("url");
        String expandedURL;

        try {
            expandedURL = expandURL(url);
            LOG.info("Expanded URL: " + expandedURL);

            if(expandedURL!=null)
                collector.emit(new Values(input.getValueByField("text"), expandedURL));
        } catch (IOException e) {
            e.printStackTrace();
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
        conn.setConnectTimeout(5000);

        String expandedURL = conn.getHeaderField("Location");

        return expandedURL;
    }
}