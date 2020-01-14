package main.tta.perf;

import com.linkedin.urls.Url;
import com.linkedin.urls.detection.UrlDetector;
import com.linkedin.urls.detection.UrlDetectorOptions;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("serial")
public class ExtractionURLBolt extends BaseRichBolt {
    private static org.apache.commons.logging.Log LOG = LogFactory.getLog(ExtractionURLBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        // Get msg from kafka spout
        String twitText = (String) input.getValueByField("str");

        // Use URL detection library from linkedin
        UrlDetector detector = new UrlDetector(twitText, UrlDetectorOptions.Default);

        // Get URL list
        List<Url> urlList = detector.detect();
        // Send each URL in list to next bolt
        for(Url url : urlList) {
            if(url.toString().contains("\"")) {
                collector.emit(new Values(twitText, url.toString().split("\"")[0]));
                LOG.info("Extract URL: " + url.toString().split("\"")[0]);
            }
            else {
                collector.emit(new Values(twitText, url.toString()));
                LOG.info("Extract URL: " + url);
            }
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "url"));
    }

}