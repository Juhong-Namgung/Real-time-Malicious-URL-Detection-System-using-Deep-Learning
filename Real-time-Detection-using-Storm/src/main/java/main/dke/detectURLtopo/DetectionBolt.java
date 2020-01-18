package main.dke.detectURLtopo;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.storm.shade.org.json.simple.JSONObject;

public class DetectionBolt extends BaseRichBolt {
    private static Log LOG = LogFactory.getLog(DetectionBolt.class);
    OutputCollector collector;

    private int[][] urlTensor = new int[1][75];
    private String modelPath;       // Deep Learning Model Path
    private Printable printable;    //
    private String dst;              // 'kafka' or 'db'

    public DetectionBolt(String path, String destination) {
        this.modelPath = path;
        this.dst = destination;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        printable = new Printable();
    }

    @Override
    public void execute(Tuple input) {
        String validURL = (String) input.getValueByField("url");
        String detectResult;

        try (SavedModelBundle b = SavedModelBundle.load(modelPath, "serve")) {
            urlTensor = printable.convert(validURL);

            //create an input Tensor
            Tensor x = Tensor.create(urlTensor);

            Session sess = b.session();

            Tensor result = sess.runner()
                    .feed("main_input_4:0", x)
                    .fetch("main_output_4/Sigmoid:0")
                    .run()
                    .get(0);

            float[][] prob = (float[][]) result.copyTo(new float[1][1]);
            LOG.info("Result value: " + prob[0][0]);

            if (prob[0][0] >= 0.5) {
                LOG.warn(validURL + " is a malicious URL!!!");
                detectResult = "malicious";
            } else {
                LOG.info(validURL + " is a benign URL!!!");
                detectResult = "benign";
            }
        }
        if(dst.equals("db")) {
            collector.emit(new Values((String) input.getValueByField("text"), validURL, detectResult, System.currentTimeMillis()));
        } else if(dst.equals("kafka")) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("text", input.getValueByField("text"));
            jsonObject.put("URL", validURL);
            jsonObject.put("result", detectResult);
            jsonObject.put("time", System.currentTimeMillis());

            collector.emit(new Values(jsonObject));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (dst.equals("db")) {
            declarer.declare(new Fields("text", "url", "result", "timestamp"));
        } else if (dst.equals("kafka")) {
            declarer.declare(new Fields("message"));
        }
    }
}