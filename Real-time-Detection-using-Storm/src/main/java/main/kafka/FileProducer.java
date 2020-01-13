package main.kafka;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.util.Properties;

public class FileProducer {

    private static Log LOG = LogFactory.getLog(FileProducer.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "print help message")
    private boolean _help = false;

    @Option(name = "--zookeeperHosts", aliases = {"--zookeeper"}, metaVar = "ZOOKEEPER HOST", usage = "path of zookeeper host")
    private static String zkHosts = "MN:42181,SN01:42181,SN02:42181,SN03:42181,SN04:42181,SN05:42181,SN06:42181,SN07:42181,SN08:42181";

    @Option(name = "--brokerList", aliases = {"--broker"}, metaVar = "BROKER LIST", usage = "path of broker list, bootstrap servers")
    private static String brokers = "MN:49092,SN01:49092,SN02:49092,SN03:49092,SN04:49092,SN05:49092,SN06:49092,SN07:49092,SN08:49092";

    @Option(name = "--inputTopic", aliases = {"--topic"}, metaVar = "INPUT TOPIC", usage = "name of kafka topic to produce data")
    private static String topic = "input";

    @Option(name = "--inputFormat", aliases = {"--format"}, metaVar = "INPUT FORMAT", usage = "format of input file")
    private static String format = "txt";

    @Option(name = "--inputFilesPath", aliases = {"--path"}, metaVar = "INPUT FILE PATH", usage = "path of input file directory")
    private static String path = "./input/";

    private static KafkaProducer<String, String> kafkaProducer;

    public FileProducer(String brokers) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    // Produce(Send) data to kafka
    public void produce(String data) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, data);
        kafkaProducer.send(producerRecord);
    }

    // Terminate Kafka producer
    public void terminate() {
        kafkaProducer.close();
    }

    // JSON format produce
    public static void jsonProduce(FileProducer fileProducer, String fileName) {
        FileInputStream fileStream;
        BufferedReader bufReader = null;
        try {
            // Construct FileInputStream and BufferedReader from InputStreamReader
            fileStream = new FileInputStream(path + fileName);
            bufReader = new BufferedReader(new InputStreamReader(fileStream));

            String line = null;

            while ((line = bufReader.readLine()) != null) {

                // Parse JsonObject from file data
                JsonObject root = new JsonParser().parse(line).getAsJsonObject();
                JsonArray subNode = root.get("data").getAsJsonArray();

                for (int i = 0; i < subNode.size(); i++) {
                    fileProducer.produce(subNode.get(i).toString());
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // File format produce
    public static void fileProduce(FileProducer fileProducer, String fileName) {
        FileInputStream fileStream;
        BufferedReader bufReader = null;
        try {
            // Construct FileInputStream and BufferedReader from InputStreamReader
            fileStream = new FileInputStream(path + "/" + fileName);
            bufReader = new BufferedReader(new InputStreamReader(fileStream));

            String line = null;

            while ((line = bufReader.readLine()) != null) {
                fileProducer.produce(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new FileProducer(brokers).doMain(args);
    }

    public void doMain(String[] args) {
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
        FileProducer fileProducer = new FileProducer(brokers);

        // Load file in path
        File file = new File(path);

        // Get files in directory
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                String fileName = f.getName();

                if (format.equals("json")) {
                    jsonProduce(fileProducer, fileName);
                } else {
                    fileProduce(fileProducer, fileName);
                }
            }
        }
        // Get one file
        else {
            if (format.equals("json")) {
                jsonProduce(fileProducer, file.getName());
            } else {
                fileProduce(fileProducer, file.getName());
            }
        }
        LOG.info("Complete kafka produce.");
    }
}