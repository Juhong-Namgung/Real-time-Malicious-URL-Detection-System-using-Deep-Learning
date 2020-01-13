package main.tta.perf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {
    private static Log LOG = LogFactory.getLog(TwitterSpout.class);

    SpoutOutputCollector _collector;
    private LinkedBlockingQueue<Status> queue;

    // API Keys
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;

    private TwitterStream twitterStream;
//    private String[] keywords = new String[]{};
    private StatusListener listener;
    private Configuration config;

    public TwitterSpout(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.queue = new LinkedBlockingQueue<Status>();
        this._collector = collector;

        // Listener Setting
        this.listener = new StatusListener() {

            public void onStatus(Status status) {
                queue.offer(status);
            }

            public void onException(Exception arg0) {
            }

            public void onTrackLimitationNotice(int arg0) {
            }

            public void onStallWarning(StallWarning arg0) {
            }

            public void onScrubGeo(long arg0, long arg1) {
            }

            public void onDeletionNotice(StatusDeletionNotice arg0) {
            }
        };

        // Setting Configuration
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(consumerKey)
                .setOAuthConsumerSecret(consumerSecret)
                .setOAuthAccessToken(accessToken)
                .setOAuthAccessTokenSecret(accessTokenSecret);
        config = cb.build();

        // twitter4j Twitter Stream
        this.twitterStream = new TwitterStreamFactory(this.config).getInstance();
        this.twitterStream.addListener(this.listener);
        this.twitterStream.sample();

        Status status;

        // Get Tweet to Status
        try {
            status = this.queue.take();
            if (status == null) {
                this.twitterStream.sample();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {

        for (long start = System.currentTimeMillis(); System.currentTimeMillis() - start < 5 * 1000; ) {

            final Status status = queue.poll();

            if (status == null) {
                Utils.sleep(50);
            } else {
                String text = status.getText();
                LOG.info(status.getText());
                _collector.emit(new Values(text));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("str"));
    }
}