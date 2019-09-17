package main.dke.detectURLtopo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus.Client;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
//import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;

import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.MyKafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


public class MainTopo {
    private static Log LOG = LogFactory.getLog(MainTopo.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "print help message")
    private boolean _help = false;

    @Option(name = "--topologyName", aliases = {"--name"}, metaVar = "TOPOLOGIE NAME", usage = "name of topology")
    private static String topologyName = "Topo";

    @Option(name = "--inputTopic", aliases = {"--input"}, metaVar = "INPUT TOPIC", usage = "name of input kafka topic")
    private static String inputTopic = "input";

    @Option(name = "--outputTopic", aliases = {"--output"}, metaVar = "OUTPUT TOPIC", usage = "name of output kafka topic")
    private static String outputTopic = "output";

    @Option(name = "--testTime", aliases = {"--t"}, metaVar = "TIME", usage = "how long should run topology")
    private static int testTime = 3;

    @Option(name = "--numWorkers", aliases = {"--workers"}, metaVar = "WORKERS", usage = "number of workers")
    private static int numWorkers = 8;

    @Option(name = "--sourceURL", aliases = {"--source"}, metaVar = "DB SOURCE URL", usage = "Source URL of MariaDB")
    private static String sourceURL = "jdbc:mysql://localhost/stormtest";

    @Option(name = "--tableName", aliases = {"--table"}, metaVar = "DB TABLE NAME", usage = "name of MariaDB table")
    private static String tableName = "test";

    @Option(name = "--dataSourceUser", aliases = {"--user"}, metaVar = "DB USER NAME", usage = "name of MariaDB user")
    private static String dataSourceUser = "root";

    @Option(name = "--dataSourcePassword", aliases = {"--password"}, metaVar = "DB USER PASSWORD", usage = "password of MariaDB user")
    private static String dataSourcePassword = "mysql";

    @Option(name = "--modelPath", aliases = {"--model"}, metaVar = "MODEL PATH", usage = "path of deep learning model")
    private static String modelPath = "./resultModel";

    public static void main(String[] args) throws NotAliveException, InterruptedException, TException {
        new MainTopo().topoMain(args);
    }

    public void topoMain(String[] args) throws InterruptedException, NotAliveException, TException {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(150);

        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            _help = true;
        }
        if (_help) {
            parser.printUsage(System.err);
            System.err.println();
            return;
        }
        if (numWorkers <= 0) {
            throw new IllegalArgumentException("Need at least one worker");
        }
        if (topologyName == null || topologyName.isEmpty()) {
            throw new IllegalArgumentException("Topology Name must be something");
        }

        String zkhosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";
        String bootstrap = "MN:9092,SN01:9092,SN02:9092,SN03:9092,SN04:9092,SN05:9092,SN06:9092,SN07:9092,SN08:9092";

			/* Kafka Spout Configuration */
        BrokerHosts brokerHosts = new ZkHosts(zkhosts);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(brokerHosts, inputTopic, "/" + inputTopic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	
			/* KafkaBolt Configuration */
        Properties props = new Properties();
        props.put("metadata.broker.list", bootstrap);
        props.put("bootstrap.servers", bootstrap);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);
        ExtractURLBolt extractBolt = new ExtractURLBolt();
        ExpandURLBolt expandBolt = new ExpandURLBolt();
        ValidateURLBolt validateBolt = new ValidateURLBolt();
        DetectBolt detectBolt = new DetectBolt(modelPath);
        ReportBolt reportBolt = new ReportBolt();

			/* KafkaBolt */
        MyKafkaBolt kafkabolt = new MyKafkaBolt().withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector(outputTopic))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

			/* JDBC Bolt */
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", sourceURL);
        hikariConfigMap.put("dataSource.user", dataSourceUser);
        hikariConfigMap.put("dataSource.password", dataSourcePassword);

        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        connectionProvider.prepare();

        List<Column> columnSchema = Lists.newArrayList(
                new Column("text", java.sql.Types.VARCHAR),
                new Column("url", java.sql.Types.VARCHAR),
                new Column("result", java.sql.Types.CHAR),
                new Column("timestamp", java.sql.Types.BIGINT)
        );

        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);

        JdbcInsertBolt insertDBBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("insert into " + tableName + " (text, url, result, timestamp) values (?,?,?,?)")
                .withQueryTimeoutSecs(30);

//			JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);			
//			JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
//			                                    .withTableName("test")
//			                                    .withQueryTimeoutSecs(30);
			
			/* Topology Build */
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout", kafkaSpout);
        builder.setBolt("extract-bolt", extractBolt).shuffleGrouping("kafka-spout");
        builder.setBolt("expand-bolt", expandBolt).shuffleGrouping("extract-bolt");
		builder.setBolt("validate-bolt", validateBolt).shuffleGrouping("expand-bolt");
		builder.setBolt("detect-bolt", detectBolt).shuffleGrouping("validate-bolt");

//        builder.setBolt("expand-bolt", expandBolt).shuffleGrouping("extract-bolt");
//        builder.setBolt("validate-bolt", validateBolt).shuffleGrouping("expand-bolt");
//        builder.setBolt("detect-bolt", detectBolt).shuffleGrouping("extract-bolt");

//        builder.setBolt("detect-bolt", detectBolt).shuffleGrouping("extract-bolt");
        builder.setBolt("kafka-bolt", kafkabolt).shuffleGrouping("detect-bolt");            // Store Data to Kafka

        //builder.setBolt("db-bolt", insertDBBolt).shuffleGrouping("detect-bolt");

        Config config = new Config();
        config.setNumWorkers(numWorkers);

        StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        printSystem();

        try {
            Thread.sleep(testTime * 60 * 1000);

            Map<String, Object> conf = Utils.readStormConfig();
            Client client = NimbusClient.getConfiguredClient(conf).getClient();
            KillOptions killOpts = new KillOptions();
            killOpts.set_wait_secs(0);
            client.killTopologyWithOpts(topologyName, killOpts);
        } catch (AlreadyAliveException ae) {
            LOG.info(ae.get_msg());
        } catch (InvalidTopologyException ie) {
            LOG.info(ie.get_msg());
        }
    }

    public static void printSystem() {
        System.out.println("");
        System.out.println("");
        System.out.println("   #     #     #     #        ###   #####   ###  #######  #     #   #####	#     #  ######   #");
        System.out.println("   ##   ##    # #    #         #   #     #   #   #     #  #     #  #     	#     #  #     #  #");
        System.out.println("   #  #  #  #     #  #         #   #         #   #     #  #     #   #####	#     #  ######   #");
        System.out.println("   #     #  #######  #         #   #         #   #     #  #     #       #	#     #  #   #    #");
        System.out.println("   #     #  #     #  #######  ###   #####   ###  #######   #####    #####	 #####   #     #  #######");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("   ######  ###### #######  ######   #####   #######  ###  #######  #     #     #####  #   #  #####  #######  ######  #     #");
        System.out.println("   #     # #         #     #        #          #      #   #     #  # #   #    #        # #   #         #     #       ##   ##");
        System.out.println("   #     # #####     #     #####    #          #      #   #     #  #  #  #     #####    #    #####     #     #####   #  #  #");
        System.out.println("   #     # #         #     #        #          #      #   #     #  #   # #          #   #         #    #     #       #     #");
        System.out.println("   ######  ######    #     ######   #####      #     ###  #######  #     #     #####    #    #####     #     ######  #     #");
        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("");
    }
}