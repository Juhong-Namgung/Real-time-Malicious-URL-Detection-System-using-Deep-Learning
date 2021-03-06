package main.tta.perf;

import main.dke.detectURLtopo.Printable;
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

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;

public class DetectionBolt extends BaseRichBolt {
    private static Log LOG = LogFactory.getLog(DetectionBolt.class);
    private OutputCollector collector;

    private int[][] urlTensor = new int[1][75];
    private String modelPath;       // Deep Learning Model Path
    private Printable printable;    //
    private SavedModelBundle b;
    private String detectResult = null;
    private Session sess;

    public DetectionBolt(String path) {
        this.modelPath = path;
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
        // Get msg from validation_bolt
        String validURL = (String) input.getValueByField("validurl");

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

        collector.emit(new Values(detectResult));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}