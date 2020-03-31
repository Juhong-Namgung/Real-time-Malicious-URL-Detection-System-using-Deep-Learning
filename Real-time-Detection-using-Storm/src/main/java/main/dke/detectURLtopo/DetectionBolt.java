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
import org.springframework.core.io.ClassPathResource;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.apache.storm.shade.org.json.simple.JSONObject;

public class DetectionBolt extends BaseRichBolt {
    private static Log LOG = LogFactory.getLog(DetectionBolt.class);
    OutputCollector collector;

    private int[][] urlTensor = new int[1][75];
    private String modelPath;       // deep learning model path
    private Printable printable;
    private SavedModelBundle b;
    private String dst;              // destination
    private Session sess;

    public DetectionBolt(String path, String destination) {
        this.modelPath = path;
        this.dst = destination;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        printable = new Printable();

        ClassPathResource resource = new ClassPathResource("models/cnn/saved_model.pb");

        try {
            File modelFile = new File("./saved_model.pb");
            IOUtils.copy(resource.getInputStream(), new FileOutputStream(modelFile));
        } catch (Exception e) {
            e.printStackTrace();
        }
        b = SavedModelBundle.load("./", "serve");
        sess = b.session();
    }

    @Override
    public void execute(Tuple input) {
        String validURL = (String) input.getValueByField("url");
        String detectResult;

        // Convert string URL to integer list
        urlTensor = printable.convert(validURL);

        //create an input Tensor
        Tensor x = Tensor.create(urlTensor);

        // Running session and get output tensor
        Tensor result = sess.runner()
                .feed("main_input_4:0", x)
                .fetch("main_output_4/Sigmoid:0")
                .run()
                .get(0);

        float[][] prob = (float[][]) result.copyTo(new float[1][1]);
        LOG.info("Result value: " + prob[0][0]);

        // Determine class(malicious or benign) use probability
        if (prob[0][0] >= 0.5) {
            LOG.warn(validURL + " is a malicious URL!!!");
            detectResult = "[ERROR] " + validURL + " is a Malicious URL!!!";
        } else {
            detectResult = "[INFO] " + validURL + " is a Benign URL!!!";
        }

//        collector.emit(new Values(detectResult));
//        collector.ack(input);

        /*try (SavedModelBundle b = SavedModelBundle.load(modelPath, "serve")) {
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
        }*/
        if(dst.equals("db")) {
            collector.emit(new Values((String) input.getValueByField("text"), validURL, detectResult, System.currentTimeMillis()));
        } else if(dst.equals("kafka")) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("text", input.getValueByField("text"));
            jsonObject.put("URL", validURL);
            jsonObject.put("result", detectResult);
            jsonObject.put("time", System.currentTimeMillis());

            collector.emit(new Values(jsonObject));
            collector.ack(input);
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