package main.tta.perf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

public class ValidationURLBolt extends BaseRichBolt {
    private static Log LOG = LogFactory.getLog(ValidationURLBolt.class);
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String expandURL = (String) input.getValueByField("expandurl");
        int httpCode;

        httpCode = validateURL(expandURL);
        if (httpCode >= 400) {
            LOG.warn("URL [" + expandURL + "] is not valid!! HTTP Status Code: " + httpCode);
        } else {
            collector.emit(new Values(input.getValueByField("text"), expandURL));
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "validurl"));
    }

    public int validateURL(String expandedURL) {
        URL url = null;
        int statusCode = 0;

        // Get HTTP Status Code
        try {
            url = new URL(expandedURL);
            HttpURLConnection http = (HttpURLConnection) url.openConnection();
            http.setRequestMethod("HEAD");
            http.setConnectTimeout(5000);
            statusCode = http.getResponseCode();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return statusCode;
    }
}