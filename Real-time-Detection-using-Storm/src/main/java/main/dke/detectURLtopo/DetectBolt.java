package main.dke.detectURLtopo;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

public class DetectBolt extends BaseRichBolt {

    OutputCollector collector;
    private int[][] urlTensor = new int[1][75];
    private String modelPath;

    private static String printable = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~ ";

    public DetectBolt(String path) {
        this.modelPath = path;
    }
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String validURL = (String) input.getValueByField("validurl");

        try (SavedModelBundle b = SavedModelBundle.load(modelPath, "serve")) {

            urlTensor = convert(validURL);
            //create an input Tensor
            Tensor x = Tensor.create(urlTensor);

            Session sess = b.session();

            Tensor result = sess.runner()
                    .feed("main_input:0", x)
                    .fetch("output/Sigmoid:0")
                    .run()
                    .get(0);

            float[][] isy = (float[][]) result.copyTo(new float[1][1]);
            System.out.println("Result value: " + isy[0][0]);

            if (isy[0][0] >= 0.5) {
                System.out.println("[WARNING] " + validURL + " is a malicious URL!!!");
                collector.emit(new Values((String) input.getValueByField("text"), validURL, "malicious", System.currentTimeMillis()));
            } else {
                System.out.println("[INFO] " + validURL + " is a benign URL!!!");
                collector.emit(new Values((String) input.getValueByField("text"), validURL, "benign", System.currentTimeMillis()));
            }
        }

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("text", "url", "result", "timestamp"));
    }

    public int[][] convert(String url) {
        int[][] result = new int[1][75];

        if (url.length() < 75) {
            for (int i = 75 - url.length(), j = 0; i < 75; i++, j++)
                result[0][i] = printable.indexOf(url.charAt(j)) + 1;
            for (int i = 0; i < 74 - url.length(); i++)
                result[0][i] = 0;
        } else {
            for (int j = 0; j < 75; j++)
                result[0][j] = printable.indexOf(url.charAt(j + url.length() - 75)) + 1;
        }
        return result;
    }
}