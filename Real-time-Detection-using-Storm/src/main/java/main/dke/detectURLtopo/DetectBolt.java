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

public class DetectBolt extends BaseRichBolt {
    private static Log LOG = LogFactory.getLog(DetectBolt.class);
    OutputCollector collector;

    private int[][] urlTensor = new int[1][75];
    private Printable printable;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        printable = new Printable();

        ClassPathResource resource = new ClassPathResource("models/cnn/saved_model.pb");

        try {
            File modelFile = new File("./saved_model.pb");
            IOUtils.copy(resource.getInputStream(),new FileOutputStream(modelFile));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        String validURL = (String) input.getValueByField("validurl");
        String detectResult;

        try (SavedModelBundle b = SavedModelBundle.load("./","serve")) {
            urlTensor = printable.convert2D(validURL);
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
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("text", input.getValueByField("text"));
        jsonObject.put("URL", validURL);
        jsonObject.put("result", detectResult);
        jsonObject.put("time", System.currentTimeMillis());

        collector.emit(new Values(jsonObject));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}