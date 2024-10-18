package argo.streaming;

import Utils.IntervalType;
import ams.connector.ArgoMessagingSink;
import ams.connector.ArgoMessagingSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;

import com.google.gson.JsonParseException;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.MDC;
import profilesmanager.EndpointGroupManager;
import profilesmanager.MetricProfileManager;
import profilesmanager.ReportManager;
import status.StatusManager;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Flink Job : Streaming status computation with multiple destinations (hbase,
 * kafka, fs) job required cli parameters --ams.endpoint : ARGO messaging api
 * endpoint to connect to msg.example.com --ams.port : ARGO messaging api port
 * --ams.token : ARGO messaging api token --ams.project : ARGO messaging api
 * project to connect to --ams.sub.metric : ARGO messaging subscription to pull
 * metric data from --ams.sub.sync : ARGO messaging subscription to pull sync
 * data from --sync.mps : metric-profile file used --sync.egp : endpoint-group
 * file used for topology --sync.aps : availability profile used --sync.ops :
 * operations profile used --sync.downtimes : initial downtime file (same for
 * run date) --report	: report name --report.uuid	: report uuid Job optional cli
 * parameters: --ams.batch : num of messages to be retrieved per request to AMS
 * service --ams.interval : interval (in ms) between AMS service requests
 * --kafka.servers : list of kafka servers to connect to --kafka.topic : kafka
 * topic name to publish events --mongo.uri : mongo uri to store latest status
 * results --mongo.method : mongo method to use (insert,upsert) --hbase.master :
 * hbase master hostname --hbase.port : hbase master.port --hbase.zk.quorum :
 * hbase zookeeper quorum --hbase.namespace : hbase namespace --hbase.table :
 * hbase table name --fs.ouput : filesystem output path (local or hdfs) mostly
 * for debugging --ams.proxy	: http proxy url --timeout : time in ms - Optional
 * timeout parameter (used in notifications) --daily : true/false - Optional
 * daily event generation parameter (not needed in notifications)
 * --url.history.endpoint(optional) the endpoint url to be used as a basis to
 * create a history url , eg ui.devel.argo.grnet.gr it can be optional , meaning
 * if it is not defined url history wont be constructed --url.help (optional)
 * the url to be used as a basis to create a help url , eg.
 * poem.egi.eu/ui/public_metrics it can be optional , meaning if it is not
 * defined url help wont be constructed --interval.loose(Optional)interval to
 * repeat events for WARNING, CRITICAL, UNKNOWN . it can be in the format of
 * DAYS, HOURS, MINUTES eg. 1h, 2d, 30m to define the period . Any of these
 * formats is transformed to minutes in the computations if not defined the
 * default value is 1440m
 * <p>
 * --interval.strict(Optional)interval to repeat events for CRITICAL . it can be
 * in the format of DAYS, HOURS, MINUTES eg. 1h, 2d, 30m to define the period .
 * Any of these formats is transformed to minutes in the computations if not
 * defined the default value is 1440m
 * <p>
 * -- latest.offset (Optional) boolean true/false, to define if the argo
 * messaging source should set offset at the latest or at the start of the
 * runDate. By default, if not defined , the offset should be the latest.
 * --level_group,level_service,level_endpoint, level_metric, if ON level alerts
 * are generated,if OFF level alerts are disabled.if no level is defined in
 * parameters then all levels are generated
 * <p>
 * --sync.interval(Optional) , the interval to sync with the argo web api source
 * (metric profiles, topology, downtimes) it can be * in the format of DAYS,
 * HOURS, MINUTES eg. 1h, 2d, 30m to define the period . By default is 24h , if
 * the parameter is not configured
 */
public class AmsStreamStatus {
    // setup logger

    static Logger LOG = LoggerFactory.getLogger(AmsStreamStatus.class);
    private static String runDate;
    private static String apiToken;
    private static String apiEndpoint;
    private static boolean level_group = true;
    private static boolean level_service = true;
    private static boolean level_endpoint = true;
    private static boolean level_metric = true;


    /**
     * Sets configuration parameters to streaming enviroment
     *
     * @param config A StatusConfig object that holds configuration parameters
     *               for this job
     * @return Stream execution enviroment
     */
    private static StreamExecutionEnvironment setupEnvironment(StatusConfig config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(config.getParameters());

        return env;
    }

    /**
     * Check if flink job has been called with ams rate params
     */
    public static boolean hasAmsRateArgs(ParameterTool paramTool) {
        String args[] = {"ams.batch", "ams.interval"};
        return hasArgs(args, paramTool);
    }

    public static boolean hasKafkaArgs(ParameterTool paramTool) {
        String kafkaArgs[] = {"kafka.servers", "kafka.topic"};
        return hasArgs(kafkaArgs, paramTool);
    }

    public static boolean hasHbaseArgs(ParameterTool paramTool) {
        String hbaseArgs[] = {"hbase.master", "hbase.master.port", "hbase.zk.quorum", "hbase.namespace",
                "hbase.table"};
        return hasArgs(hbaseArgs, paramTool);
    }

    public static boolean hasFsOutArgs(ParameterTool paramTool) {
        String fsOutArgs[] = {"fs.output"};
        return hasArgs(fsOutArgs, paramTool);
    }

    public static boolean hasMongoArgs(ParameterTool paramTool) {
        String mongoArgs[] = {"mongo.uri", "mongo.method"};
        return hasArgs(mongoArgs, paramTool);
    }

    public static boolean hasArgs(String[] reqArgs, ParameterTool paramTool) {

        for (String reqArg : reqArgs) {
            if (!paramTool.has(reqArg)) {
                return false;
            }
        }

        return true;
    }

    public static boolean hasAmsPubArgs(ParameterTool paramTool) {
        String amsPubArgs[] = {"ams.project.publish", "ams.token.publish", "ams.notification.topic"};
        return hasArgs(amsPubArgs, paramTool);
    }

    public static boolean hasAmsArgs(ParameterTool paramTool) {
        String amsPubArgs[] = {"ams.project.publish", "ams.token.publish", "ams.alert.topic"};
        return hasArgs(amsPubArgs, paramTool);
    }


    /**
     * Main dataflow of flink job
     */
    public static void main(String[] args) throws Exception {

        configJID();
        // Initialize cli parameter tool
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StatusConfig conf = new StatusConfig(parameterTool);

        StreamExecutionEnvironment see = setupEnvironment(conf);
        see.setParallelism(1);

        // Initialize Input Source : ARGO Messaging Source
        String endpoint = parameterTool.getRequired("ams.endpoint");
        String port = parameterTool.get("ams.port");
        String token = parameterTool.getRequired("ams.token");
        String project = parameterTool.getRequired("ams.project");
        String subMetric = parameterTool.getRequired("ams.sub.metric");

        level_group = !isOFF(parameterTool, "level_group");
        level_service = !isOFF(parameterTool, "level_service");
        level_endpoint = !isOFF(parameterTool, "level_endpoint");
        level_metric = !isOFF(parameterTool, "level_metric");


        apiEndpoint = parameterTool.getRequired("api.endpoint");
        apiToken = parameterTool.getRequired("api.token");
        String reportID = parameterTool.getRequired("report.uuid");
        Long apiInterval = parameterTool.getLong("api.interval");
        runDate = parameterTool.get("run.date");
        if (runDate != null) {
            runDate = runDate + "T00:00:00.000Z";
        }

        int looseInterval = 1440;
        int strictInterval = 1440;

        if (parameterTool.has("interval.loose")) {
            String looseParam = parameterTool.get("interval.loose");
            looseInterval = getInterval(looseParam);
        }

        if (parameterTool.has("interval.strict")) {
            String strictParam = parameterTool.get("interval.strict");
            strictInterval = getInterval(strictParam);
        }
        ApiResourceManager amr = new ApiResourceManager(apiEndpoint, apiToken);

        // fetch
        // set params
        if (parameterTool.has("proxy")) {
            amr.setProxy(parameterTool.get("proxy"));
        }

        if (parameterTool.has("api.timeout")) {
            amr.setTimeoutSec(parameterTool.getInt("api.timeout"));
        }

        amr.setReportID(reportID);
        amr.getRemoteAll();

        // set ams client batch and interval to default values
        int batch = 1;
        long interval = 100L;

        if (hasAmsRateArgs(parameterTool)) {
            batch = parameterTool.getInt("ams.batch");
            interval = parameterTool.getLong("ams.interval");
        }

        // Establish the metric data AMS stream
        // Ingest sync avro encoded data from AMS endpoint
        String offsetDt = null;
        if (parameterTool.has("latest.offset") && !parameterTool.getBoolean("latest.offset")) {
            offsetDt = runDate;
        }
        String syncInterval = null;
        if (parameterTool.has("sync.interval")) {
            syncInterval = parameterTool.get("sync.interval");
        }
        ArgoMessagingSource amsMetric = new ArgoMessagingSource(endpoint, port, token, project, subMetric, batch, interval, offsetDt);
        ArgoApiSource apiSync = new ArgoApiSource(apiEndpoint, apiToken, reportID, syncInterval, apiInterval);
        if (parameterTool.has("ams.verify")) {
            boolean verify = parameterTool.getBoolean("ams.verify");
            amsMetric.setVerify(verify);

        }

        if (parameterTool.has("proxy")) {
            String proxyURL = parameterTool.get("proxy");
            amsMetric.setProxy(proxyURL);
            apiSync.setProxy(proxyURL);
        }

        DataStream<String> metricAMS = see.addSource(amsMetric).setParallelism(1);

        // Establish the sync stream from argowebapi
        DataStream<Tuple2<String, String>> syncAMS = see.addSource(apiSync).setParallelism(1);

        // Forward syncAMS data to two paths
        // - one with parallelism 1 to connect in the first processing step and
        // - one with max parallelism for status event generation step
        // (scalable)
        DataStream<Tuple2<String, String>> syncA = syncAMS.forward();
        DataStream<Tuple2<String, String>> syncB = syncAMS.broadcast();

        DataStream<Tuple2<String, MetricData>> groupMdata = metricAMS.connect(syncA)
                .flatMap(new MetricDataWithGroup(conf)).setParallelism(1);

        DataStream<String> events = groupMdata.connect(syncB).flatMap(new StatusMap(conf, looseInterval, strictInterval, level_group, level_service, level_endpoint, level_metric));
        if (hasKafkaArgs(parameterTool)) {
            // Initialize kafka parameters
            String kafkaServers = parameterTool.get("kafka.servers");
            String kafkaTopic = parameterTool.get("kafka.topic");
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("bootstrap.servers", kafkaServers);
//            FlinkKafkaProducer09<String> kSink = new FlinkKafkaProducer09<String>(kafkaTopic, new SimpleStringSchema(),
//                    kafkaProps);
            KafkaRecordSerializationSchema serializer = KafkaRecordSerializationSchema.builder()
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .setTopic(kafkaTopic)
                    .build();
            KafkaSink<String> kSink = KafkaSink.<String>builder()
                    .setBootstrapServers(kafkaServers)
                    .setRecordSerializer(serializer)
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
            events.sinkTo(kSink);
        } else if (hasAmsArgs(parameterTool)) {
            String topic = parameterTool.get("ams.alert.topic");
            String tokenpub = parameterTool.get("ams.token.publish");
            String projectpub = parameterTool.get("ams.project.publish");

            ArgoMessagingSink ams = new ArgoMessagingSink(endpoint, port, tokenpub, projectpub, topic, interval, runDate);
            if (parameterTool.has("proxy")) {
                String proxyURL = parameterTool.get("proxy");
                ams.setProxy(proxyURL);
            }
            events.addSink(ams);
        }

        if (hasHbaseArgs(parameterTool)) {
            // Initialize Output : Hbase Output Format
            HBaseOutputFormat hbf = new HBaseOutputFormat();
            hbf.setMaster(parameterTool.get("hbase.master"));
            hbf.setMasterPort(parameterTool.get("hbase.master.port"));
            hbf.setZkQuorum(parameterTool.get("hbase.zk.quorum"));
            hbf.setZkPort(parameterTool.get("hbase.zk.port"));
            hbf.setNamespace(parameterTool.get("hbase.namespace"));
            hbf.setTableName(parameterTool.get("hbase.table"));
            hbf.setReport(parameterTool.get("report"));
            events.writeUsingOutputFormat(hbf);
        }

        if (hasMongoArgs(parameterTool)) {

            MongoStatusOutput mongoOut = new MongoStatusOutput(parameterTool.get("mongo.uri"), "status_metrics",
                    "status_endpoints", "status_services", "status_endpoint_groups", parameterTool.get("mongo.method"),
                    parameterTool.get("report.uuid"));
            events.writeUsingOutputFormat(mongoOut);
        }

        if (hasFsOutArgs(parameterTool)) {
            events.writeAsText(parameterTool.get("fs.output"), FileSystem.WriteMode.OVERWRITE);
            //events.print();
        }
        if (hasAmsPubArgs(parameterTool)) {
            String topic = parameterTool.get("ams.notification.topic");
            String tokenpub = parameterTool.get("ams.token.publish");
            String projectpub = parameterTool.get("ams.project.publish");

            ArgoMessagingSink ams = new ArgoMessagingSink(endpoint, port, tokenpub, projectpub, topic, interval, runDate);
            if (parameterTool.has("proxy")) {
                String proxyURL = parameterTool.get("proxy");
                ams.setProxy(proxyURL);
            }
            events = events.flatMap(new TrimEvent(parameterTool, amr.getTenant(), amr.getReportName(), amr.getEgroup()));
            events.addSink(ams);
        }

        // Create a job title message to discern job in flink dashboard/cli
        StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Streaming status using data from ");
        jobTitleSB.append(endpoint);
        jobTitleSB.append(":");
        jobTitleSB.append(port);
        jobTitleSB.append("/v1/projects/");
        jobTitleSB.append(project);
        jobTitleSB.append("/subscriptions/");
        jobTitleSB.append(subMetric);

        // Execute flink dataflow
        see.execute(jobTitleSB.toString());
    }

    /**
     * MetricDataWithGroup implements a map function that adds group information
     * to the metric data message
     */
    private static class MetricDataWithGroup extends RichCoFlatMapFunction<String, Tuple2<String, String>, Tuple2<String, MetricData>> {

        private static final long serialVersionUID = 1L;

        public EndpointGroupManager egp;
        public MetricProfileManager mps;

        public StatusConfig config;
        private ApiResourceManager amr;

        public MetricDataWithGroup(StatusConfig config) {
            LOG.info("Created new Status map");
            this.config = config;
        }

        /**
         * Initializes constructs in the beginning of operation
         *
         * @param parameters Configuration parameters to initialize structures
         * @throws URISyntaxException
         */
        @Override
        public void open(Configuration parameters) throws IOException, ParseException, URISyntaxException {
            try {
                // Initialize the API resource manager
                this.amr = new ApiResourceManager(config.apiEndpoint, config.apiToken);
                this.amr.setDate(config.runDate);
                this.amr.setTimeoutSec((int) config.timeout);

                // Set proxy if provided
                if (config.apiProxy != null) {
                    this.amr.setProxy(config.apiProxy);
                }

                // Set report ID and fetch remote data
                this.amr.setReportID(config.reportID);
                this.amr.getRemoteAll();

                // Load metric profiles
                ArrayList<MetricProfile> mpsList = new ArrayList<>(Arrays.asList(this.amr.getListMetrics()));
                mps = new MetricProfileManager();
                mps.loadFromList(mpsList);

                if (mps.getProfiles().isEmpty()) {
                    LOG.warn("No metric profiles found after loading from API.");
                    return; // Exit early if no profiles are available
                }

                // Get valid metric profile and services
                String validMetricProfile = mps.getProfiles().get(0);
                ArrayList<String> validServices = mps.getProfileServices(validMetricProfile);

                // Load and trim group endpoints
                ArrayList<GroupEndpoint> egpList = new ArrayList<>(Arrays.asList(this.amr.getListGroupEndpoints()));
                ArrayList<GroupEndpoint> egpTrim = new ArrayList<>();

                for (GroupEndpoint egpItem : egpList) {
                    if (validServices.contains(egpItem.getService())) {
                        egpTrim.add(egpItem);
                    }
                }

                // Load trimmed endpoints into the group manager
                egp = new EndpointGroupManager();
                egp.loadFromList(egpTrim);
                LOG.info("Loaded {} valid group endpoints.", egpTrim.size());

            } catch (Exception e) {
                LOG.error("Unexpected error while initializing: {}", e.getMessage(), e);
                // Handle any other unexpected exceptions
            }
        }

        /**
         * The main flat map function that accepts metric data and generates
         * metric data with group information
         *
         * @param value Input metric data in base64 encoded format from AMS
         *              service
         * @param out   Collection of generated Tuple2<MetricData,String> objects
         */
        @Override
        public void flatMap1(String value, Collector<Tuple2<String, MetricData>> out)
                throws IOException, ParseException {

            try {
                // Create JSON parser and parse the input string
                JsonParser jsonParser = new JsonParser();
                JsonElement jRoot = jsonParser.parse(value);

                // Extract the "data" field and decode it
                String data = jRoot.getAsJsonObject().get("data").getAsString();
                byte[] decoded64 = Base64.decodeBase64(data.getBytes(StandardCharsets.UTF_8));

                // Set up Avro reader and decode the data
                DatumReader<MetricData> avroReader = new SpecificDatumReader<>(MetricData.getClassSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(decoded64, null);
                MetricData item = avroReader.read(null, decoder);

                // Extract service and hostname from the MetricData
                String service = item.getService();
                String hostname = item.getHostname();

                // Get groups associated with the service and hostname
                ArrayList<String> groups = egp.getGroup(hostname, service);

                for (String groupItem : groups) {
                    Tuple2<String, MetricData> curItem = new Tuple2<>();
                    curItem.f0 = groupItem;
                    curItem.f1 = item;
                    out.collect(curItem);
                    LOG.info("Item enriched: {}", curItem);
                }

            } catch (JsonParseException e) {
                LOG.error("JSON parsing error: {}", e.getMessage(), e);
            } catch (IOException e) {
                LOG.error("IO error during processing: {}", e.getMessage(), e);
                throw e; // Rethrow if necessary to propagate the error
            } catch (Exception e) {
                LOG.error("Unexpected error in flatMap1: {}", e.getMessage(), e);
            }
        }

        public void flatMap2(Tuple2<String, String> value, Collector<Tuple2<String, MetricData>> out)
                throws IOException, ParseException {

            try {
                if (value.f0.equalsIgnoreCase("metric_profile")) {
                    // Update metric profiles
                    ArrayList<MetricProfile> mpsList = SyncParse.parseMetricJSON(value.f1);
                    mps = new MetricProfileManager();
                    mps.loadFromList(mpsList);
                    LOG.info("Updated metric profiles: {}", mpsList);

                } else if (value.f0.equalsIgnoreCase("group_endpoints")) {
                    // Update endpoint groups
                    ArrayList<GroupEndpoint> egpList = SyncParse.parseGroupEndpointJSON(value.f1);
                    egp = new EndpointGroupManager();

                    String validMetricProfile = mps.getProfiles().get(0);
                    ArrayList<String> validServices = mps.getProfileServices(validMetricProfile);

                    // Trim endpoint groups based on valid services
                    ArrayList<GroupEndpoint> egpTrim = new ArrayList<>();
                    for (GroupEndpoint egpItem : egpList) {
                        if (validServices.contains(egpItem.getService())) {
                            egpTrim.add(egpItem);
                        }
                    }

                    // Load the trimmed endpoint groups into the manager
                    egp.loadFromList(egpTrim);
                    LOG.info("Updated endpoint groups: {}", egpTrim);

                } else {
                    LOG.warn("Unknown value type: {}", value.f0);
                }
            } catch (Exception e) {
                LOG.error("Unexpected error in flatMap2: {}", e.getMessage(), e);
            }
        }

    }

    /**
     * StatusMap implements a rich flat map function which holds status
     * information for all entities in topology and for each received metric
     * generates the appropriate status events
     */
    private static class StatusMap extends RichCoFlatMapFunction<Tuple2<String, MetricData>, Tuple2<String, String>, String> {

        private static final long serialVersionUID = 1L;

        private String pID;

        public StatusManager sm;

        public StatusConfig config;

        public int initStatus;
        public int looseInterval;
        public int strictInterval;
        private ApiResourceManager amr;
        boolean level_group;
        boolean level_service;
        boolean level_endpoint;
        boolean level_metric;

        public StatusMap(StatusConfig config, int looseInterval, int strictInterval, boolean level_group, boolean level_service, boolean level_endpoint, boolean level_metric) {
            LOG.info("Created new Status map");
            this.config = config;
            this.looseInterval = looseInterval;
            this.strictInterval = strictInterval;
            this.level_group = level_group;
            this.level_service = level_service;
            this.level_endpoint = level_endpoint;
            this.level_metric = level_metric;

        }

        /**
         * Initializes constructs in the beginning of operation
         *
         * @param parameters Configuration parameters to initialize structures
         * @throws URISyntaxException
         */
        @Override
        public void open(Configuration parameters) throws IOException, ParseException, URISyntaxException {
            pID = Integer.toString(getRuntimeContext().getIndexOfThisSubtask());

            this.amr = new ApiResourceManager(config.apiEndpoint, config.apiToken);
            this.amr.setDate(config.runDate);
            this.amr.setTimeoutSec((int) config.timeout);

            if (config.apiProxy != null) {
                this.amr.setProxy(config.apiProxy);
            }

            this.amr.setReportID(config.reportID);

            // Try to fetch all remote resources and handle potential exceptions
            try {
                this.amr.getRemoteAll();
            } catch (Exception e) {
                LOG.error("Failed to fetch remote resources: {}", e.getMessage());
                // You can choose to return or continue with defaults if necessary
                return; // Or handle defaults accordingly
            }

            ArrayList<String> opsList = new ArrayList<>();
            ArrayList<String> apsList = new ArrayList<>();
            ArrayList<Downtime> downList = new ArrayList<>();
            ArrayList<MetricProfile> mpsList = new ArrayList<>();
            ArrayList<GroupEndpoint> egpListFull = new ArrayList<>();

            try {
                String opsJSON = this.amr.getResourceJSON(ApiResource.OPS);
                opsList.add(opsJSON);
            } catch (Exception e) {
                LOG.error("Failed to get operations JSON: {}", e.getMessage());
            }

            try {
                String apsJSON = this.amr.getResourceJSON(ApiResource.AGGREGATION);
                apsList.add(apsJSON);
            } catch (Exception e) {
                LOG.error("Failed to get aggregation JSON: {}", e.getMessage());
            }

            try {
                downList = new ArrayList<>(Arrays.asList(this.amr.getListDowntimes()));
            } catch (Exception e) {
                LOG.error("Failed to get downtimes: {}", e.getMessage());
            }

            try {
                mpsList = new ArrayList<>(Arrays.asList(this.amr.getListMetrics()));
            } catch (Exception e) {
                LOG.error("Failed to get metric profiles: {}", e.getMessage());
            }

            try {
                egpListFull = new ArrayList<>(Arrays.asList(this.amr.getListGroupEndpoints()));
            } catch (Exception e) {
                LOG.error("Failed to get group endpoints: {}", e.getMessage());
            }

            // create a new status manager
            sm = new StatusManager();
            sm.setLooseInterval(looseInterval);
            sm.setStrictInterval(strictInterval);
            sm.setReport(config.report);
            sm.setGroupType(this.amr.getEgroup());

            // Load all connector data with error checking
            try {
                sm.loadAll(config.runDate, downList, egpListFull, mpsList, apsList, opsList);
            } catch (Exception e) {
                LOG.error("Failed to load all connector data: {}", e.getMessage(), e);
            }

            sm.setLevel_group(level_group);
            sm.setLevel_service(level_service);
            sm.setLevel_endpoint(level_endpoint);
            sm.setLevel_metric(level_metric);

            // Set the default status as integer and handle potential errors
            try {
                initStatus = sm.getOps().getIntStatus(config.initStatus);
            } catch (Exception e) {
                LOG.error("Failed to get initial status: {}", e.getMessage(), e);
                // Optionally, set a default status value if required
                //   initStatus = -1; // or some other default value
            }

            LOG.info("Initialized status manager: {} (with critical timeout: {} and warning/unknown/missing timeout: {})",
                    pID, sm.getStrictInterval(), sm.getLooseInterval());
        }

        /**
         * The main flat map function that accepts metric data and generates
         * status events
         *
         * @param value Input metric data in base64 encoded format from AMS
         *              service
         * @param out   Collection of generated status events as json strings
         */
        @Override
        public void flatMap1(Tuple2<String, MetricData> value, Collector<String> out) throws IOException, ParseException {
            MetricData item = value.f1;
            String group = value.f0;

            // Extract fields from MetricData
            String service = item.getService();
            String hostname = item.getHostname();
            String metric = item.getMetric();
            String status = item.getStatus();
            String tsMon = item.getTimestamp();
            String monHost = item.getMonitoringHost();
            String message = item.getMessage();
            String summary = item.getSummary();
            String dayStamp = tsMon.split("T")[0];

            // Handle downtime data retrieval with error checking
            try {
                if (!sm.checkIfExistDowntime(dayStamp)) {
                    this.amr.setDate(dayStamp);
                    this.amr.getRemoteDowntimes();
                    ArrayList<Downtime> downList = new ArrayList<>(Arrays.asList(this.amr.getListDowntimes()));
                    sm.addDowntimeSet(dayStamp, downList);
                }
            } catch (Exception e) {
                LOG.error("Failed to retrieve or process downtimes for day {}: {}", dayStamp, e.getMessage(), e);
            }

            // Check if daily generation is enabled and if the day has changed
            try {
                if (config.daily && sm.hasDayChanged(sm.getTsLatest(), tsMon)) {
                    ArrayList<String> eventsDaily = sm.dumpStatus(tsMon);
                    sm.setTsLatest(tsMon);
                    for (String event : eventsDaily) {
                        out.collect(event);
                        LOG.info("sm-{}: daily event produced: {}", pID, event);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error during daily event generation: {}", e.getMessage(), e);
            }

            // Check if group is handled by this operator instance
            try {
                if (!sm.hasGroup(group)) {
                    // Get start of the day to create new entries
                    Date dateTS = sm.setDate(tsMon);
                    sm.addNewGroup(group, initStatus, dateTS);
                }
            } catch (Exception e) {
                LOG.error("Failed to add new group {}: {}", group, e.getMessage(), e);
            }

            // Set the status and collect events
            try {
                ArrayList<String> events = sm.setStatus(group, service, hostname, metric, status, monHost, tsMon, summary, message);

                for (String event : events) {
                    out.collect(event);
                    LOG.info("sm-{}: event produced: {}", pID, item);
                }
            } catch (Exception e) {
                LOG.error("Error setting status for group {}: {}", group, e.getMessage(), e);
            }
        }

        public void flatMap2(Tuple2<String, String> value, Collector<String> out) throws IOException, ParseException {
            try {
                if (value.f0.equalsIgnoreCase("metric_profile")) {
                    // Update mps
                    ArrayList<MetricProfile> mpsList = SyncParse.parseMetricJSON(value.f1);
                    sm.mps = new MetricProfileManager();
                    sm.mps.loadFromList(mpsList);
                    LOG.info("Updated metric profiles: {}", mpsList.size());

                } else if (value.f0.equals("group_endpoints")) {
                    // Update egp
                    ArrayList<GroupEndpoint> egpList = SyncParse.parseGroupEndpointJSON(value.f1);
                    String validMetricProfile = sm.mps.getProfiles().get(0);
                    ArrayList<String> validServices = sm.mps.getProfileServices(validMetricProfile);

                    // Trim profile services
                    ArrayList<GroupEndpoint> egpTrim = new ArrayList<>();
                    for (GroupEndpoint egpItem : egpList) {
                        if (validServices.contains(egpItem.getService())) {
                            egpTrim.add(egpItem);
                        }
                    }

                    // Load next topology into a temporary endpoint group manager
                    EndpointGroupManager egpNext = new EndpointGroupManager();
                    egpNext.loadFromList(egpTrim);

                    // Update topology using the temporary endpoint group manager
                    sm.updateTopology(egpNext);
                    LOG.info("Updated group endpoints: {}", egpTrim.size());

                } else if (value.f0.equalsIgnoreCase("downtimes")) {
                    String pDate = Instant.now().toString().split("T")[0];
                    ArrayList<Downtime> downList = SyncParse.parseDowntimesJSON(value.f1);
                    // Update downtime cache in status manager
                    sm.addDowntimeSet(pDate, downList);
                    LOG.info("Updated downtimes: {}", downList.size());

                } else {
                    LOG.warn("Unknown value type received: {}", value.f0);
                }
            } catch (Exception e) {
                LOG.error("Unexpected error occurred while processing {}: {}", value.f0, e.getMessage(), e);
            }
        }
    }

    /**
     * HbaseOutputFormat implements a custom output format for storing results
     * in hbase
     */
    private static class HBaseOutputFormat implements OutputFormat<String> {

        private String report = null;
        private String master = null;
        private String masterPort = null;
        private String zkQuorum = null;
        private String zkPort = null;
        private String namespace = null;
        private String tname = null;
        private Connection connection = null;
        private Table ht = null;

        private static final long serialVersionUID = 1L;

        // Setters
        public void setMasterPort(String masterPort) {
            this.masterPort = masterPort;
        }

        public void setMaster(String master) {
            this.master = master;
        }

        public void setZkQuorum(String zkQuorum) {
            this.zkQuorum = zkQuorum;
        }

        public void setZkPort(String zkPort) {
            this.zkPort = zkPort;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public void setTableName(String tname) {
            this.tname = tname;
        }

        public void setReport(String report) {
            this.report = report;
        }

        @Override
        public void configure(Configuration parameters) {

        }

        /**
         * Structure initialization
         */
        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            // Create hadoop based configuration for hclient to use
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
            // Modify configuration to job needs
            config.setInt("timeout", 120000);
            if (masterPort != null && !masterPort.isEmpty()) {
                config.set("hbase.master", master + ":" + masterPort);
            } else {
                config.set("hbase.master", master + ":60000");
            }

            config.set("hbase.zookeeper.quorum", zkQuorum);
            config.set("hbase.zookeeper.property.clientPort", (zkPort));
            // Create the connection
            connection = ConnectionFactory.createConnection(config);
            if (namespace != null) {
                ht = connection.getTable(TableName.valueOf(namespace + ":" + tname));
            } else {
                ht = connection.getTable(TableName.valueOf(tname));
            }

        }

        /**
         * Extract json representation as string to be used as a field value
         */
        private String extractJson(String field, JsonObject root) {
            JsonElement el = root.get(field);
            if (el != null && !(el.isJsonNull())) {

                return el.getAsString();

            }
            return "";
        }

        /**
         * Accepts status event as json string and stores it in hbase table
         *
         * @parameter record A string with json represantation of a status event
         */
        @Override
        public void writeRecord(String record) throws IOException {

            JsonParser jsonParser = new JsonParser();
            // parse the json root object
            JsonObject jRoot = jsonParser.parse(record).getAsJsonObject();
            // Get fields

            String rep = this.report;
            String tp = extractJson("type", jRoot);
            String dt = extractJson("date", jRoot);
            String eGroup = extractJson("endpoint_group", jRoot);
            String service = extractJson("service", jRoot);
            String hostname = extractJson("hostname", jRoot);
            String metric = extractJson("metric", jRoot);
            String status = extractJson("status", jRoot);
            String prevStatus = extractJson("prev_status", jRoot);
            String prevTs = extractJson("prev_ts", jRoot);
            String tsm = extractJson("ts_monitored", jRoot);
            String tsp = extractJson("ts_processed", jRoot);

            // Compile key
            // Key is constructed based on
            // report > metric_type > date(day) > endpoint group > service >
            // hostname > metric
            String key = rep + "|" + tp + "|" + dt + "|" + eGroup + "|" + service + "|" + hostname + "|" + metric + "|"
                    + tsm;

            // Prepare columns
            Put put = new Put(Bytes.toBytes(key));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("report"), Bytes.toBytes(rep));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("type"), Bytes.toBytes(tp));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("endpoint_group"), Bytes.toBytes(eGroup));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("service"), Bytes.toBytes(service));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("hostname"), Bytes.toBytes(hostname));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("metric"), Bytes.toBytes(metric));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("status"), Bytes.toBytes(status));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("prev_status"), Bytes.toBytes(prevStatus));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("prev_ts"), Bytes.toBytes(prevTs));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ts_monitored"), Bytes.toBytes(tsm));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ts_processed"), Bytes.toBytes(tsp));

            // Insert row in hbase
            ht.put(put);

        }

        /**
         * Closes hbase table and hbase connection
         */
        @Override
        public void close() throws IOException {
            ht.close();
            connection.close();
        }
    }

    public static class IntervalStruct {

        IntervalType intervalType;
        int intervalValue;

        public IntervalStruct(IntervalType intervalType, int intervalValue) {
            this.intervalType = intervalType;
            this.intervalValue = intervalValue;
        }

        public IntervalType getIntervalType() {
            return intervalType;
        }

        public void setIntervalType(IntervalType intervalType) {
            this.intervalType = intervalType;
        }

        public int getIntervalValue() {
            return intervalValue;
        }

        public void setIntervalValue(int intervalValue) {
            this.intervalValue = intervalValue;
        }

    }

    public static IntervalStruct parseInterval(String intervalParam) {

        String regex = "[0-9]*[h,d,m]$";
        boolean matches = intervalParam.matches(regex);
        int intervalValue = 1440;
        IntervalType intervalType = null;

        if (matches) {

            String intervals[] = new String[]{};
            if (intervalParam.contains("h")) {
                intervalType = IntervalType.HOURS;
                intervals = intervalParam.split("h");
            } else if (intervalParam.contains("d")) {
                intervalType = IntervalType.DAY;
                intervals = intervalParam.split("d");

            } else if (intervalParam.contains("m")) {
                intervalType = IntervalType.MINUTES;
                intervals = intervalParam.split("m");
            }
            if (intervalType != null && StringUtils.isNumeric(intervals[0])) {
                int interval = Integer.parseInt(intervals[0]);
                switch (intervalType) {
                    case DAY:
                        intervalValue = interval * 24 * 60;
                        break;
                    case HOURS:
                        intervalValue = interval * 60;
                        break;
                    case MINUTES:
                        intervalValue = interval;
                        break;
                    default:
                        intervalValue = 1440;
                        break;
                }

            }

        }
        return new IntervalStruct(intervalType, intervalValue);

    }

    public static int getInterval(String intervalParam) {

        IntervalStruct intervalStruct = parseInterval(intervalParam);

        return intervalStruct.getIntervalValue();

    }


    private static String getJID() {
        return JobID.generate().toString();
    }

    private static void configJID() { //config the JID in the log4j.properties
        String jobId = getJID();
        MDC.put("JID", jobId);
    }

    public static boolean isOFF(ParameterTool params, String paramName) {
        if (params.has(paramName)) {
            return params.get(paramName).equals("OFF");

        } else {
            return false;
        }
    }

}
