package main.dke.detectURLtopo;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

@SuppressWarnings("serial")
public class ExtractionURLBolt extends BaseRichBolt {
    private static org.apache.commons.logging.Log LOG = LogFactory.getLog(ExtractionURLBolt.class);
    OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String twitText = (String) input.getValueByField("str");
        UrlDetector detector = new UrlDetector(twitText, UrlDetectorOptions.Default);

        List<Url> urlList = detector.detect();
        for(Url url : urlList) {
            collector.emit(new Values(twitText, url.toString()));
            LOG.info("Extract URL: " + url);
        }
//        String url = null;
//
//        // extract URL using regex matching
//        url = extractURL(twitText);
//
//        if (!url.equals("")) {
//            collector.emit(new Values(twitText, url));
//            System.out.println("Extract URL: " + url);
//        }
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