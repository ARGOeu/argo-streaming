package argo.streaming;

import ams.connector.ArgoMessagingSource;
import java.util.concurrent.TimeUnit;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;

import com.google.gson.JsonParser;

import argo.avro.MetricData;
import argo.avro.MetricDataOld;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.ConcatFileCompactor;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.OutputStreamBasedFileCompactor;
import org.apache.flink.connector.file.sink.compactor.RecordWiseFileCompactor;
import org.apache.flink.connector.file.sink.compactor.SimpleStringDecoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.slf4j.MDC;

/**
 * Flink Job : Stream metric data from ARGO messaging to Hbase job required cli
 * parameters:
 *
 * --ams.endpoint : ARGO messaging api endoint to connect to msg.example.com
 * --ams.port : ARGO messaging api port --ams.token : ARGO messaging api token
 * --ams.project : ARGO messaging api project to connect to --ams.sub : ARGO
 * messaging subscription to pull from --hbase.master : hbase endpoint
 * --hbase.master.port : hbase master port --hbase.zk.quorum : comma separated
 * list of hbase zookeeper servers --hbase.zk.port : port used by hbase
 * zookeeper servers --hbase.namespace : table namespace used (usually tenant
 * name) --hbase.table : table name (usually metric_data) --check.path :
 * checkpoint path --check.interval : checkpoint interval --hdfs.path : hdfs
 * destination to write the data --ams.batch : num of messages to be retrieved
 * per request to AMS service --ams.interval : interval (in ms) between AMS
 * service requests --ams.proxy : optional http proxy url --ams.verify :
 * optional turn on/off ssl verify
 */
public class AmsIngestMetric {
    // setup logger

    static Logger LOG = LoggerFactory.getLogger(AmsIngestMetric.class);
    private static String runDate;
    private static String basePath;

    /**
     * Check if flink job has been called with ams rate params
     */
    public static boolean hasAmsRateArgs(ParameterTool paramTool) {
        String args[] = {"ams.batch", "ams.interval"};
        return hasArgs(args, paramTool);
    }

    /**
     * Check if flink job has been called with checkpoint cli arguments
     */
    public static boolean hasCheckArgs(ParameterTool paramTool) {
        String args[] = {"check.path", "check.interval"};
        return hasArgs(args, paramTool);
    }

    /**
     * Check if flink job has been called with hdfs cli arguments
     */
    public static boolean hasHdfsArgs(ParameterTool paramTool) {
        String args[] = {"hdfs.path"};
        return hasArgs(args, paramTool);
    }

    /**
     * Check if flink job has been called with hbase cli arguments
     */
    public static boolean hasHbaseArgs(ParameterTool paramTool) {
        String args[] = {"hbase.master", "hbase.master.port", "hbase.zk.quorum", "hbase.zk.port", "hbase.namespace",
            "hbase.table"};
        return hasArgs(args, paramTool);
    }

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

    public static void main(String[] args) throws Exception {

        configJID();
        // Create flink execution environment
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        // On failure attempt max 10 times to restart with a retry interval of 2 minutes
        see.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(2, TimeUnit.MINUTES)));
      //  see.enableCheckpointing(Duration.ofMinutes(5).toMillis());

        // Initialize cli parameter tool
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // set ams client batch and interval to default values
        int batch = 1;
        long interval = 100L;
        long inactivityThresh = 1800000L; // default inactivity threshold value ~ 30mins

        if (hasAmsRateArgs(parameterTool)) {
            batch = parameterTool.getInt("ams.batch");
            interval = parameterTool.getLong("ams.interval");
        }

        // Initialize Input Source : ARGO Messaging Source
        String endpoint = parameterTool.getRequired("ams.endpoint");
        String port = parameterTool.get("ams.port");
        String token = parameterTool.getRequired("ams.token");
        String project = parameterTool.getRequired("ams.project");
        String sub = parameterTool.getRequired("ams.sub");
        runDate = parameterTool.get("run.date");
        if (runDate != null) {
            runDate = runDate + "T00:00:00.000Z";
        }

        // Check if checkpointing is desired
        if (hasCheckArgs(parameterTool)) {
            String checkPath = parameterTool.get("check.path");
            String checkInterval = parameterTool.get("check.interval");
            // Establish check-pointing mechanism using the cli-parameter check.path
            see.setStateBackend(new HashMapStateBackend());
            see.getCheckpointConfig().setCheckpointStorage(checkPath);

            // Establish the check-pointing interval
            long checkInt = Long.parseLong(checkInterval);
            see.enableCheckpointing(checkInt);
            
            System.out.println("checkpoint");
        }

        // Ingest sync avro encoded data from AMS endpoint
        ArgoMessagingSource ams = new ArgoMessagingSource(endpoint, port, token, project, sub, batch, interval, runDate);

        if (parameterTool.has("ams.verify")) {
            ams.setVerify(parameterTool.getBoolean("ams.verify"));
        }

        if (parameterTool.has("ams.proxy")) {
            ams.setProxy(parameterTool.get("ams.proxy"));
        }

        DataStream<String> metricDataJSON = see.addSource(ams);
        DataStream<MetricData> metricDataPOJO = metricDataJSON.flatMap(new FlatMapFunction<String, MetricData>() {

            /**
             * Flat Map Function that accepts AMS message and exports the metric
             * data object (encoded in the payload)
             */
            private static final long serialVersionUID = 1L;

            @Override
            public void flatMap(String value, Collector<MetricData> out) throws Exception {

                JsonParser jsonParser = new JsonParser();
                // parse the json root object
                JsonElement jRoot = jsonParser.parse(value);
                // parse the json field "data" and read it as string
                // this is the base64 string payload
                String data = jRoot.getAsJsonObject().get("data").getAsString();
                // Decode from base64
                byte[] decoded64 = Base64.decodeBase64(data.getBytes("UTF-8"));
                // Decode from avro

                DatumReader<MetricData> avroReader = new SpecificDatumReader<MetricData>(MetricData.getClassSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);

                MetricData item;
                try {
                    item = avroReader.read(null, decoder);
                } catch (java.io.EOFException ex) {
                    //convert from old to new
                    avroReader = new SpecificDatumReader<MetricData>(MetricDataOld.getClassSchema(), MetricData.getClassSchema());
                    decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
                    item = avroReader.read(null, decoder);
                }
                if (item != null) {
                    LOG.info("Captured data -- {}", item.toString());
                    out.collect(item);
                }

            }
        });

        // Check if saving to HDFS is desired
        if (hasHdfsArgs(parameterTool)) {
            basePath = parameterTool.getRequired("hdfs.path");
            // Establish a bucketing sink to be able to store events with different daily
            // timestamp parts (YYYY-MM-DD)
            // in different daily files
//            BucketingSink<MetricData> bs = new BucketingSink<MetricData>(basePath);
//            bs.setInactiveBucketThreshold(inactivityThresh);
//            Bucketer<MetricData> tsBuck = new TSBucketer();
//            bs.setBucketer(tsBuck);
//            bs.setPartPrefix("mdata");
//            // Change default in progress prefix: _ to allow loading the file in
//            // AvroInputFormat
//            bs.setInProgressPrefix("");
//            // Add .prog extension when a file is in progress mode
//            bs.setInProgressSuffix(".prog");
//            // Add .pend extension when a file is in pending mode
//            bs.setPendingSuffix(".pend");
//
//            bs.setWriter(new SpecificAvroWriter<MetricData>());
//            metricDataPOJO.addSink(bs);
//        }

            OutputFileConfig config = OutputFileConfig
                    .builder()
                    .withPartPrefix("mdata")
                    .build();
            FileCompactStrategy strategy = FileCompactStrategy.Builder.newBuilder()
                    .enableCompactionOnCheckpoint(1)
                    .build();
            // RecordWiseFileCompactor compactor = new RecordWiseFileCompactor<>(new DecoderBasedReader.Factory<>(SimpleStringDecoder::new));
            OutputStreamBasedFileCompactor compactor2 = new ConcatFileCompactor();
         
          FileSink<MetricData> sink = FileSink.forBulkFormat(new Path(basePath), AvroWriters.forReflectRecord(MetricData.class))
                    .withRollingPolicy(new CustomOnCheckpointRollingPolicy(Duration.ofMinutes(5).toMillis(), inactivityThresh))
                    .withBucketAssigner(new CustomBucketAssigner())
                    .enableCompact(strategy, compactor2)
                    .withOutputFileConfig(config)
                    .build();

            metricDataPOJO.sinkTo(sink).uid(getJID());

        }
        // Check if saving to Hbase is desired
        if (hasHbaseArgs(parameterTool)) {
            // Initialize Output : Hbase Output Format
            HBaseMetricOutputFormat hbf = new HBaseMetricOutputFormat();
            hbf.setMaster(parameterTool.getRequired("hbase.master"));
            hbf.setMasterPort(parameterTool.getRequired("hbase.master-port"));
            hbf.setZkQuorum(parameterTool.getRequired("hbase.zk.quorum"));
            hbf.setZkPort(parameterTool.getRequired("hbase.zk.port"));
            hbf.setNamespace(parameterTool.getRequired("hbase.namespace"));
            hbf.setTableName(parameterTool.getRequired("hbase.table"));

            metricDataPOJO.writeUsingOutputFormat(hbf);
        }

        // Create a job title message to discern job in flink dashboard/cli
        StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Ingesting metric data from ");
        jobTitleSB.append(endpoint);
        jobTitleSB.append(":");
        jobTitleSB.append(port);
        jobTitleSB.append("/v1/projects/");
        jobTitleSB.append(project);
        jobTitleSB.append("/subscriptions/");
        jobTitleSB.append(sub);

        see.execute(jobTitleSB.toString());
    }

    private static String getJID() {
        return JobID.generate().toString();
    }

    private static void configJID() { //config the JID in the log4j.properties
        String jobId = getJID();
        MDC.put("JID", jobId);
    }

    private static class CustomBucketAssigner extends DateTimeBucketAssigner<MetricData> {

        @Override
        public String getBucketId(MetricData element, Context context) {
            String dailyPart = element.getTimestamp().split("T")[0];
            //       Path path = new Path(basePath + "/" + dailyPart);
            //    return path.toString() ;
            return "/" + dailyPart;
        }

    }

    private static class CustomOnCheckpointRollingPolicy<IN, BucketID> extends CheckpointRollingPolicy<IN, BucketID> {

        private static final long serialVersionUID = 1L;

        private static long rolloverInterval;

        private static long inactivityInterval;

        private CustomOnCheckpointRollingPolicy(long rolloverIntervalVal, long inactivityIntervalVal) {
            rolloverInterval = rolloverIntervalVal;
            inactivityInterval = inactivityIntervalVal;
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<BucketID> partFileState, IN element) {
            return false;
        }

        @Override
        public boolean shouldRollOnProcessingTime(PartFileInfo<BucketID> partFileState, long currentTime) {
            return currentTime - partFileState.getCreationTime() >= rolloverInterval
                    || currentTime - partFileState.getLastUpdateTime() >= inactivityInterval;
        }

        public static <IN, BucketID> CustomOnCheckpointRollingPolicy<IN, BucketID> build() {
            return new CustomOnCheckpointRollingPolicy<>(rolloverInterval, inactivityInterval);
        }
    }
}
