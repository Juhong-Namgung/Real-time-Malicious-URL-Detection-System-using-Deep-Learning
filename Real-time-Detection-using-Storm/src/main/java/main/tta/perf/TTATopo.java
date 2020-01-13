package main.tta.perf;

import main.dke.detectURLtopo.ExpansionURLBolt;
import main.dke.detectURLtopo.ValidationURLBolt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.MyKafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class TTATopo {
    private static Log LOG = LogFactory.getLog(TTATopo.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "print help message")
    private boolean _help = false;

    @Option(name = "--topologyName", aliases = {"--name"}, metaVar = "TOPOLOGIE NAME", usage = "name of topology")
    private static String topologyName = "Topo";

    @Option(name = "--zookeeperHosts", aliases = {"--zookeeper"}, metaVar = "ZOOKEEPER HOST", usage = "path of zookeeper host")
    private static String zkHosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";

    @Option(name = "--brokerList", aliases = {"--broker"}, metaVar = "BROKER LIST", usage = "path of broker list, bootstrap servers")
    private static String brokers = "MN:49092,SN01:49092,SN02:49092,SN03:49092,SN04:49092,SN05:49092,SN06:49092,SN07:49092,SN08:49092";

    @Option(name = "--numWorkers", aliases = {"--workers"}, metaVar = "WORKERS", usage = "number of workers")
    private static int numWorkers = 8;

    @Option(name = "--inputTopic", aliases = {"--input"}, metaVar = "INPUT TOPIC", usage = "name of kafka topic to produce data")
    private static String inputTopic = "input";

    @Option(name = "--outputTopic", aliases = {"--output"}, metaVar = "OUTPUT TOPIC", usage = "name of kafka topic to consume data")
    private static String outputTopic = "output";

    @Option(name = "--modelPath", aliases = {"--model"} , metaVar = "TENSORFLOW MODEL PATH", usage ="path of deep learning model")
    private static String modelPath = "./models/";

    @Option(name = "--parallelismHint", aliases = {"--parm"}, metaVar = "PARALLELISM HINT", usage = "number of spout, bolts(KafkaSpout-ExtractBolt-ExpandBolt-ValidateBolt-DetectBolt-KafkaBolt")
    private static String paralleism = "1 2 4 1";

    @Option(name = "--testTime", aliases = {"--t"}, metaVar = "TIME", usage = "how long should run topology")
    private static int testTime = 3;

    @Option(name = "--consumerKey", aliases = {"--key"}, metaVar = "KEY", usage = "Twitter Consumer Key")
    private static String consumerKey = "consumerKey";

    @Option(name = "--consumerSecret", aliases = {"--secret"}, metaVar = "SECRET", usage = "Twitter Consumer Secret")
    private static String consumerSecret = "consumerSecret";

    @Option(name = "--accessToken", aliases = {"--token"}, metaVar = "TOKEN", usage = "Twitter Access Token")
    private static String accessToken = "accessToken";

    @Option(name = "--accessTokenSecret", aliases = {"--tsecret"}, metaVar = "TOKEN SECRET", usage = "Twitter Access Token Secret")
    private static String accessTokenSecret = "accessTokenSecret";

    /*
     * Topology architecture
     * 1) KafkaSpout or InputSpout
     * 2) ExtractBolt
     * 3) DetectBolt
     * 4) KafkaBolt or ReportBolt
     *
     */
    private static final String TOPOLOGY_NAME = "TTA_Performance_Topo";
    private static final String KAFKA_SPOUT_ID = "KafkaSpout";
    private static final String TWITTER_SPOUT_ID = "TwitterSpout";
    private static final String EXTRACTION_BOLT_ID = "ExtractionBolt";
    private static final String EXPANSION_BOLT_ID = "ExpansionBolt";
    private static final String VALIDATION_BOLT_ID = "ValidationBolt";
    private static final String DETECTION_BOLT_ID = "DetectionBolt";
    private static final String KAFKA_BOLT_ID = "KafkaBolt";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        new TTATopo().doMain(args);
    }

    public void doMain(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(150);

        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            LOG.error(e.getMessage());
            _help = true;
        }
        if (_help) {
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        	/* Kafka Spout Configuration */
        BrokerHosts brokerHosts = new ZkHosts(zkHosts);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, inputTopic, "/" + inputTopic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

			/* KafkaBolt Configuration */
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("bootstrap.servers", brokers);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);
        TwitterSpout twitterSpout = new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        ExtractionURLBolt extractionBolt = new ExtractionURLBolt();
        ExpansionURLBolt expansionBolt = new ExpansionURLBolt();
        ValidationURLBolt validationBolt = new ValidationURLBolt();
        DetectionBolt detectionBolt = new DetectionBolt(modelPath);

			/* KafkaBolt */
        MyKafkaBolt kafkabolt = new MyKafkaBolt().withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(outputTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

        	/* Topology Build */
        TopologyBuilder builder = new TopologyBuilder();

        ArrayList<Integer> parameters = new ArrayList<Integer>();
        String[] params = paralleism.split(" ");

        for(String p : params) {
            parameters.add(Integer.parseInt(p));
        }

//        builder.setSpout(INPUT_SPOUT_ID, kafkaSpout, parameters.get(0));
        builder.setSpout(TWITTER_SPOUT_ID, twitterSpout, parameters.get(0));
        builder.setBolt(EXTRACTION_BOLT_ID, extractionBolt, parameters.get(1)).shuffleGrouping(TWITTER_SPOUT_ID);
        builder.setBolt(EXPANSION_BOLT_ID, expansionBolt, parameters.get(2)).shuffleGrouping(EXTRACTION_BOLT_ID);
        builder.setBolt(VALIDATION_BOLT_ID, validationBolt, parameters.get(3)).shuffleGrouping(EXPANSION_BOLT_ID);
        builder.setBolt(DETECTION_BOLT_ID, detectionBolt, parameters.get(4)).shuffleGrouping(VALIDATION_BOLT_ID);
        builder.setBolt(KAFKA_BOLT_ID, kafkabolt, parameters.get(5)).shuffleGrouping(DETECTION_BOLT_ID);            // Store Data to Kafka

        Config config = new Config();
        config.setNumWorkers(numWorkers);

        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());

        try {
            Thread.sleep(testTime * 60 * 1000);

            Map<String, Object> conf = Utils.readStormConfig();
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(0);
            client.killTopologyWithOpts(topologyName, killOpts);
        } catch (AlreadyAliveException ae) {
            LOG.info(ae.get_msg());
        } catch (InvalidTopologyException ie) {
            LOG.info(ie.get_msg());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (NotAliveException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
