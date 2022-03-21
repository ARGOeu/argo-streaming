package ams.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AmsPublisher implements a flink connector to messaging system
 * parameters: 
 * --ams.endpoint : ARGO messaging api endoint to connect to
 * msg.example.com 
 * --ams.port : ARGO messaging api port
 * --ams.token : ARGO  messaging  api token of publisher
 * --ams.project : ARGO messaging api project to connect to
 * --ams.topic : ARGO messaging topic to publish messages
 * --ams.verify : optional turn on/off ssl verify
 * --ams.interval : optional interval to timeout connection
 
 */
public class AmsPublisher {

    // setup logger
    static Logger LOG = LoggerFactory.getLogger(AmsPublisher.class);

    /**
     * Check if a list of expected cli arguments have been provided to this
     * flink job
     */
    public static boolean hasArgs(String[] reqArgs, ParameterTool paramTool) {

        for (String reqArg : reqArgs) {
            if (!paramTool.has(reqArg)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if flink job has been called with ams rate params
     */
    public static boolean hasAmsRateArgs(ParameterTool paramTool) {
        String args[] = {"ams.interval"};
        return hasArgs(args, paramTool);
    }

    // main job function
    public static void main(String[] args) throws Exception {

        // Create flink execution enviroment
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        // Fixed restart strategy: on failure attempt max 10 times to restart with a retry interval of 2 minutes
        see.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(2, TimeUnit.MINUTES)));
        // Initialize cli parameter tool
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // Initialize Input Source : ARGO Messaging Source
        String endpoint = parameterTool.getRequired("ams.endpoint");
        String port = parameterTool.get("ams.port");
        String token = parameterTool.getRequired("ams.token");
        String project = parameterTool.getRequired("ams.project");
        String topic = parameterTool.getRequired("ams.topic");

        // set ams client batch and interval to default values
        //int batch = 1;
        long interval = 100L;

        if (hasAmsRateArgs(parameterTool)) {
            interval = parameterTool.getLong("ams.interval");
        }

        //Ingest sync avro encoded data from AMS endpoint
        ArgoMessagingSink ams = new ArgoMessagingSink(endpoint, port, token, project, topic, interval);

        if (parameterTool.has("ams.verify")) {
            ams.setVerify(parameterTool.getBoolean("ams.verify"));
        }

        if (parameterTool.has("ams.proxy")) {
            ams.setProxy(parameterTool.get("ams.proxy"));
        }

        List<String> events = new ArrayList<>();
        int i;
        for (i = 0; i < 10; i++) {
            events.add("hello world! _ "+i);
        }
        DataStream<String> eventstreams = see.fromCollection(events);
        eventstreams.addSink(ams);

        // Create a job title message to discern job in flink dashboard/cli
        StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Publish  data to AMS ");
        jobTitleSB.append(endpoint);
        jobTitleSB.append(":");
        jobTitleSB.append(port);
        jobTitleSB.append("/v1/projects/");
        jobTitleSB.append(project);
        jobTitleSB.append("/topic/");
        jobTitleSB.append(topic);

        see.execute(jobTitleSB.toString());

    }

}
