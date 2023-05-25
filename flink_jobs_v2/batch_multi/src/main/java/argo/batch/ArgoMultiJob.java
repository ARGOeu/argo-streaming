package argo.batch;

import argo.amr.ApiResource;
import argo.amr.ApiResourceManager;
import argo.ar.CalcEndpointAR;
import argo.ar.CalcGroupAR;
import argo.ar.CalcServiceAR;
import argo.ar.EndpointAR;
import argo.ar.EndpointGroupAR;
import argo.ar.ServiceAR;
import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import argo.avro.Weight;
import trends.calculations.ServiceTrends;
import trends.flipflops.ZeroServiceFlipFlopFilter;
import trends.status.EndpointTrendsCounter;
import trends.calculations.CalcEndpointFlipFlopTrends;
import trends.calculations.CalcGroupFlipFlopTrends;
import trends.calculations.CalcMetricFlipFlopTrends;
import trends.calculations.CalcServiceFlipFlopTrends;
import trends.calculations.EndpointTrends;
import trends.calculations.GroupTrends;
import trends.flipflops.MapEndpointTrends;
import trends.flipflops.MapGroupTrends;
import trends.flipflops.MapMetricTrends;
import trends.flipflops.MapServiceTrends;
import trends.calculations.MetricTrends;
import trends.calculations.MongoTrendsOutput;
import trends.status.StatusAndDurationFilter;
import trends.calculations.Trends;
import trends.flipflops.ZeroEndpointFlipFlopFilter;
import trends.flipflops.ZeroGroupFlipFlopFilter;
import trends.flipflops.ZeroMetricFlipFlopFilter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profilesmanager.ReportManager;
import trends.status.GroupTrendsCounter;
import trends.status.MetricTrendsCounter;
import trends.status.ServiceTrendsCounter;
import utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.JobID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.MDC;

/**
 * Implements an ARGO Status Batch Job in flink
 * <p>
 * Submit job in flink cluster using the following parameters: --pdata: path to
 * previous day's metric data file (For hdfs use:
 * hdfs://namenode:port/path/to/file) --mdata: path to metric data file (For
 * hdfs use: hdfs://namenode:port/path/to/file) --run.date: target date of
 * computation in DD-MM-YYYY format --mongo.uri: path to MongoDB destination (eg
 * mongodb://localhost:27017/database.table --mongo.method: Method for storing
 * results to Mongo (insert,upsert) --report.id: UUUID of the report
 * --api.endpoint: endpoint hostname of the argo-web-api instance
 * (api.argo.example.com) --api.token: access token to argo-web-api --api.proxy:
 * optional address for proxy to be used (http://proxy.example.com)
 * --api.timeout: set timeout (in seconds) when connecting to argo-web-api
 * --calcStatus(Optional): set to be OFF or ON . ON to calculate and write
 * status timelines , OFF to not calculate. If not set status timeline
 * calculations will be defined from the report --calcAR(Optional): set to be
 * OFF or ON . ON to calculate and write a/r results, OFF to not calculate . If
 * not set a/r calculations will be defined from the report
 * --calcFlipFlops(Optional): set to be OFF or ON . ON to calculate and write
 * flipflop trends , OFF to not calculate. If not set flip flop trends
 * calculations will be defined from the report --calcStatusTrends(Optional):
 * set to be OFF or ON . ON to calculate and write status trends timelines in
 * mongo db , OFF to not calculate. If not set status trends calculations will
 * be defined from the report --calcTagTrends(Optional): set to be OFF or ON .
 * ON to calculate and write tags results in mongo db , OFF to not calculate .
 * If not set tags calculations will be defined from the report
 * 
 * 
 * --source-data(Optinal): tenant, all, feeds-only . Defines from where the source data can be received. if not defined or is tenant the data are received from the tenant (the tenant does not combine data), if value is feeds-only the data are received from 
 * the tenant list defined in the feeds data(the tenant is acting as a gatherer without data on it's own), if is all the source data are received both from the tenant and the tenants list in the feeds data (the tenant already exists and has data to combine)
 * 
 * 
 * --source-topo(Optinal): tenant, all . Defines from where the source topology can be received. if not defined or is tenant the data are received from the tenant (the tenant either is not combining data or acts as a gatherer without topologies on its own), 
 * , if is all the source data are received both from the tenant and the tenants list in the feeds data and needs a parameter to be added to the api request to receive the topology (the tenant has it's own topologies)
 */
public class ArgoMultiJob {

    static Logger LOG = LoggerFactory.getLogger(ArgoMultiJob.class);

    private static String dbURI;
    private static String reportID;
    private static Integer rankNum;
    private static boolean clearMongo = false;

    private static String runDate;

    private static boolean calcStatus = false;
    private static boolean calcAR = false;
    private static boolean calcStatusTrends = false;
    private static boolean calcFlipFlops = false;
    private static boolean calcTagTrends = false;
    
    enum Combined {
        TENANT("tenant"),
        ALL("all"), 
        FEEDS_ONLY("feeds-only");  
        public final String label;

        private Combined(String label) {
            this.label = label;
        }
        
    }
  
    
    private static Enum sourceData=Combined.TENANT;
    private static Enum sourceTopo=Combined.TENANT;

    public static void main(String[] args) throws Exception {

        configJID();
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        DateTime now = new DateTime(DateTimeZone.UTC);
        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        if (params.get("clearMongo") != null && params.getBoolean("clearMongo") == true) {
            clearMongo = true;
        }
        
        if(params.get("source-data")!=null && params.get("source-data").equals(Combined.ALL.label)){
           sourceData=Combined.ALL;
        }else if(params.get("source-data")!=null && params.get("source-data").equals(Combined.FEEDS_ONLY.label)){
           sourceData=Combined.FEEDS_ONLY;
        }
        
        if(params.get("source-topo")!=null && params.get("source-topo").equals(Combined.ALL.label)){
           sourceTopo=Combined.ALL;
        }      
        
        boolean isCombined=false;
        boolean isSourceAll=false;
        
        if (!sourceData.equals(Combined.TENANT)) { //in case source-data is not TENANT, the tenant is a combined one
            isCombined = true;
        }
        
        if (sourceTopo.equals(Combined.ALL)) { //in case source-topo is ALL, we need to add &mode=combined to api request
            isSourceAll = true;
        }
        String apiEndpoint = params.getRequired("api.endpoint");
        String apiToken = params.getRequired("api.token");
        reportID = params.getRequired("report.id");

        ApiResourceManager amr = new ApiResourceManager(apiEndpoint, apiToken);

        // fetch
        // set params
        if (params.has("api.proxy")) {
            amr.setProxy(params.get("api.proxy"));
        }

        if (params.has("api.timeout")) {
            amr.setTimeoutSec(params.getInt("api.timeout"));
        }
        runDate = params.getRequired("run.date");
        amr.setIsCombined(isCombined);
        amr.setIsSourceTopoAll(isSourceAll);
        amr.setReportID(reportID);
        amr.setDate(runDate);
        amr.getRemoteAll();

        DataSource<String> confDS = env.fromElements(amr.getResourceJSON(ApiResource.CONFIG));
        // Get conf data 
        List<String> confData = confDS.collect();
        ReportManager cfgMgr = new ReportManager();
        cfgMgr.loadJsonString(confData);

        enableComputations(cfgMgr.activeComputations, params);
        if (isCombined) {
            amr.getTenant();
        }

        DataSource<String> opsDS = env.fromElements(amr.getResourceJSON(ApiResource.OPS));
        DataSource<String> apsDS = env.fromElements(amr.getResourceJSON(ApiResource.AGGREGATION));
        DataSource<String> recDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.RECOMPUTATIONS) != null) {
            recDS = env.fromElements(amr.getResourceJSON(ApiResource.RECOMPUTATIONS));
        }

        DataSource<String> mtagsDS = env.fromElements("");
        if (amr.getResourceJSON(ApiResource.MTAGS) != null) {

            mtagsDS = env.fromElements(amr.getResourceJSON(ApiResource.MTAGS));
        }
        DataSet<Weight> weightDS = env.fromElements(new Weight());
        Weight[] listWeights = amr.getListWeights();

        if (listWeights.length > 0) {
            weightDS = env.fromElements(amr.getListWeights());
        }

        DataSet<Downtime> downDS = env.fromElements(new Downtime());
        // begin with empty threshold datasource
        DataSource<String> thrDS = env.fromElements("");
        // check if report information from argo-web-api contains a threshold profile ID
        if (!amr.getThresholdsID().equalsIgnoreCase("")) {
            // grab information about thresholds rules from argo-web-api
            thrDS = env.fromElements(amr.getResourceJSON(ApiResource.THRESHOLDS));
        }

        // Get conf data
        DataSet<MetricProfile> mpsDS = env.fromElements(amr.getListMetrics());
        DataSet<GroupEndpoint> egpDS = env.fromElements(amr.getListGroupEndpoints());
        DataSet<GroupGroup> ggpDS = env.fromElements(new GroupGroup());

        DataSet<MetricProfile> nempsDS = env.fromElements(new MetricProfile("","","",null));
        MetricProfile[] nempsList = amr.getNewEntriesMetrics();
        if (nempsList.length > 0) {
            nempsDS = env.fromElements(nempsList);
        }
        GroupGroup[] listGroups = amr.getListGroupGroups();
        if (listGroups.length > 0) {
            ggpDS = env.fromElements(amr.getListGroupGroups());
        }
        Downtime[] listDowntimes = amr.getListDowntimes();
        if (listDowntimes.length > 0) {
            downDS = env.fromElements(amr.getListDowntimes());
        }

        List<String> tenantList = new ArrayList<>();
        if(sourceData.equals(Combined.TENANT) || sourceData.equals(Combined.ALL)){
            tenantList.add(amr.getTenant());
        }
      
        if(sourceData.equals(Combined.FEEDS_ONLY) || sourceData.equals(Combined.ALL)){   
            tenantList.addAll(Arrays.asList(amr.getListTenants()));
        } 
        
        List<Path[]> tenantPaths = new ArrayList<>();

        DateTime currentDate = Utils.convertStringtoDate("yyyy-MM-dd", runDate);
        String previousDate = Utils.convertDateToString("yyyy-MM-dd", currentDate.minusDays(1));
          for (String tenant : tenantList) {
            Path[] paths = new Path[2];
            if (isCombined) {
                paths[0] = new Path(params.getRequired("basispath") + "/" + tenant + "/mdata/" + runDate);
                paths[1] = new Path(params.getRequired("basispath") + "/" + tenant + "/mdata/" + previousDate);
                tenantPaths.add(paths);
            } else {
                paths[0] = new Path(params.getRequired("mdata"));
                paths[1] = new Path(params.getRequired("pdata"));
                tenantPaths.add(paths);

            }
        }
          DataSet<MetricData> allMetricData = null;
        for (Path[] path : tenantPaths) {
            // todays metric data
            Path in = path[0];
            AvroInputFormat<MetricData> mdataAvro = new AvroInputFormat(in, MetricData.class);
            DataSet<MetricData> mdataDS = env.createInput(mdataAvro);

            // previous metric data
            Path pin = path[1];
            AvroInputFormat<MetricData> pdataAvro = new AvroInputFormat(pin, MetricData.class);
            DataSet<MetricData> pdataDS = env.createInput(pdataAvro);

            DataSet<MetricData> pdataCleanDS = pdataDS.flatMap(new ExcludeMetricData()).withBroadcastSet(recDS, "rec");

            // Find the latest day
            DataSet<MetricData> pdataMin = pdataCleanDS.groupBy("service", "hostname", "metric")
                    .sortGroup("timestamp", Order.DESCENDING).first(1);

            // Union todays data with the latest statuses from previous day
            DataSet<MetricData> mdataPrevTotalDS = mdataDS.union(pdataMin);

            if (allMetricData == null) {
                allMetricData = mdataPrevTotalDS;
            } else {
                allMetricData = allMetricData.union(mdataPrevTotalDS);
            }
        }
        // Use yesterday's latest statuses and todays data to find the missing ones and add them to the mix
        DataSet<StatusMetric> fillMissDS = allMetricData.reduceGroup(new FillMissing(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(opsDS, "ops").withBroadcastSet(confDS, "conf").withBroadcastSet(nempsDS, "nemps");

        // Discard unused data and attach endpoint group as information
        DataSet<StatusMetric> mdataTrimDS = allMetricData.flatMap(new PickEndpoints(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(recDS, "rec").withBroadcastSet(confDS, "conf").withBroadcastSet(thrDS, "thr")
                .withBroadcastSet(opsDS, "ops").withBroadcastSet(apsDS, "aps");

        // Combine prev and todays metric data with the generated missing metric
        // data
        DataSet<StatusMetric> mdataTotalDS = mdataTrimDS.union(fillMissDS);

        mdataTotalDS = mdataTotalDS.flatMap(new MapServices()).withBroadcastSet(apsDS, "aps");
        dbURI = params.getRequired("mongo.uri");
        String dbMethod = params.getRequired("mongo.method");

        // Create status detail data set
        DataSet<StatusMetric> stDetailDS = mdataTotalDS.groupBy("group", "service", "hostname", "metric")
                .sortGroup("timestamp", Order.ASCENDING).reduceGroup(new CalcPrevStatus(params))
                .withBroadcastSet(mpsDS, "mps").withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops");

        //Create StatusMetricTimeline dataset for endpoints
        DataSet<StatusTimeline> statusMetricTimeline = stDetailDS.groupBy("group", "service", "hostname", "metric").sortGroup("timestamp", Order.ASCENDING)
                .reduceGroup(new CalcMetricTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(apsDS, "aps");

        //Create StatusMetricTimeline dataset for endpoints
        DataSet<StatusTimeline> statusEndpointTimeline = statusMetricTimeline.groupBy("group", "service", "hostname")
                .reduceGroup(new CalcEndpointTimeline(params, now)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps").withBroadcastSet(downDS, "down");

        DataSet<StatusTimeline> statusServiceTimeline = statusEndpointTimeline.groupBy("group", "service")
                .reduceGroup(new CalcServiceTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps");

        DataSet<StatusTimeline> statusEndGroupFunctionTimeline = statusServiceTimeline.groupBy("group", "function")
                .reduceGroup(new CalcGroupFunctionTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps");

        DataSet<StatusTimeline> statusGroupTimeline = statusEndGroupFunctionTimeline.groupBy("group")
                .reduceGroup(new CalcGroupTimeline(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                .withBroadcastSet(apsDS, "aps");

        if (calcStatus) {
            //Calculate endpoint timeline timestamps             

            stDetailDS = stDetailDS.flatMap(new MapStatusMetricTags(calcTagTrends)).withBroadcastSet(mtagsDS, "mtags").withBroadcastSet(egpDS, "egp")
                    .withBroadcastSet(confDS, "conf");
            DataSet<StatusMetric> stEndpointDS = statusEndpointTimeline.flatMap(new CalcStatusEndpoint(params)).withBroadcastSet(mpsDS, "mps").withBroadcastSet(opsDS, "ops")
                    .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(confDS, "conf");
// Create status service data set

            DataSet<StatusMetric> stServiceDS = statusServiceTimeline.flatMap(new CalcStatusService(params)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp").withBroadcastSet(opsDS, "ops")
                    .withBroadcastSet(apsDS, "aps");

// Create status endpoint group data set
            DataSet<StatusMetric> stEndGroupDS = statusGroupTimeline.flatMap(new CalcStatusEndGroup(params))
                    .withBroadcastSet(mpsDS, "mps").withBroadcastSet(egpDS, "egp").withBroadcastSet(ggpDS, "ggp")
                    .withBroadcastSet(opsDS, "ops").withBroadcastSet(apsDS, "aps");

            // Initialize four mongo outputs (metric,endpoint,service,endpoint_group)
            MongoStatusOutput metricMongoOut = new MongoStatusOutput(dbURI, "status_metrics", dbMethod, MongoStatusOutput.StatusType.STATUS_METRIC, reportID, runDate, clearMongo);
            MongoStatusOutput endpointMongoOut = new MongoStatusOutput(dbURI, "status_endpoints", dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT, reportID, runDate, clearMongo);
            MongoStatusOutput serviceMongoOut = new MongoStatusOutput(dbURI, "status_services", dbMethod, MongoStatusOutput.StatusType.STATUS_SERVICE, reportID, runDate, clearMongo);
            MongoStatusOutput endGroupMongoOut = new MongoStatusOutput(dbURI, "status_endpoint_groups", dbMethod, MongoStatusOutput.StatusType.STATUS_ENDPOINT_GROUP, reportID, runDate, clearMongo);

            stDetailDS.output(metricMongoOut);
            stEndpointDS.output(endpointMongoOut);
            stServiceDS.output(serviceMongoOut);
            stEndGroupDS.output(endGroupMongoOut);

        }

        if (calcAR) {
            //Calculate endpoint a/r 
            DataSet<EndpointAR> endpointArDS = statusEndpointTimeline.flatMap(new CalcEndpointAR(params)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(opsDS, "ops").withBroadcastSet(egpDS, "egp").
                    withBroadcastSet(ggpDS, "ggp").withBroadcastSet(confDS, "conf");
            //Calculate endpoint timeline timestamps 

            DataSet<ServiceAR> serviceArDS = statusServiceTimeline.flatMap(new CalcServiceAR(params)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(opsDS, "ops").withBroadcastSet(egpDS, "egp").
                    withBroadcastSet(ggpDS, "ggp").withBroadcastSet(confDS, "conf");

            // DataSet<StatusTimeline> statusGroupTimelineAR = statusGroupTimeline.flatMap(new ExcludeGroupMetrics(params)).withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops");
            DataSet<EndpointGroupAR> endpointGroupArDS = statusGroupTimeline.flatMap(new CalcGroupAR(params, now)).withBroadcastSet(mpsDS, "mps")
                    .withBroadcastSet(apsDS, "aps").withBroadcastSet(opsDS, "ops").withBroadcastSet(egpDS, "egp").
                    withBroadcastSet(ggpDS, "ggp").withBroadcastSet(confDS, "conf").withBroadcastSet(weightDS, "weight").withBroadcastSet(recDS, "rec");

            MongoEndpointArOutput endpointARMongoOut = new MongoEndpointArOutput(dbURI, "endpoint_ar", dbMethod, reportID, runDate, clearMongo);
            MongoServiceArOutput serviceARMongoOut = new MongoServiceArOutput(dbURI, "service_ar", dbMethod, reportID, runDate, clearMongo);
            MongoEndGroupArOutput endGroupARMongoOut = new MongoEndGroupArOutput(dbURI, "endpoint_group_ar", dbMethod, reportID, runDate, clearMongo);
            endpointArDS.output(endpointARMongoOut);
            serviceArDS.output(serviceARMongoOut);
            endpointGroupArDS.output(endGroupARMongoOut);
        }

        if (calcFlipFlops || calcStatusTrends) {
            DataSet<MetricTrends> metricTrends = statusMetricTimeline.flatMap(new CalcMetricFlipFlopTrends());
            DataSet<EndpointTrends> endpointTrends = statusEndpointTimeline.flatMap(new CalcEndpointFlipFlopTrends());
            DataSet<ServiceTrends> serviceTrends = statusServiceTimeline.flatMap(new CalcServiceFlipFlopTrends());
            DataSet<GroupTrends> groupTrends = statusGroupTimeline.flatMap(new CalcGroupFlipFlopTrends());
            if (calcFlipFlops) {
                DataSet<MetricTrends> noZeroMetricFlipFlops = metricTrends.filter(new ZeroMetricFlipFlopFilter());
                if (rankNum != null) { //sort and rank data
                    noZeroMetricFlipFlops = noZeroMetricFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    noZeroMetricFlipFlops = noZeroMetricFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }

                MongoTrendsOutput metricFlipFlopMongoOut = new MongoTrendsOutput(dbURI, "flipflop_trends_metrics", MongoTrendsOutput.TrendsType.TRENDS_METRIC, reportID, runDate, clearMongo);
                DataSet<Trends> trends = noZeroMetricFlipFlops.map(new MapMetricTrends(calcTagTrends)).withBroadcastSet(mtagsDS, "mtags");
                trends.output(metricFlipFlopMongoOut);

                DataSet<EndpointTrends> nonZeroEndpointFlipFlops = endpointTrends.filter(new ZeroEndpointFlipFlopFilter());

                if (rankNum != null) { //sort and rank data
                    nonZeroEndpointFlipFlops = nonZeroEndpointFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    nonZeroEndpointFlipFlops = nonZeroEndpointFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }

                MongoTrendsOutput endpointFlipFlopMongoOut = new MongoTrendsOutput(dbURI, "flipflop_trends_endpoints", MongoTrendsOutput.TrendsType.TRENDS_ENDPOINT, reportID, runDate, clearMongo);
                trends = nonZeroEndpointFlipFlops.map(new MapEndpointTrends());
                trends.output(endpointFlipFlopMongoOut);
                DataSet<ServiceTrends> noZeroServiceFlipFlops = serviceTrends.filter(new ZeroServiceFlipFlopFilter());

                if (rankNum != null) { //sort and rank data
                    noZeroServiceFlipFlops = noZeroServiceFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    noZeroServiceFlipFlops = noZeroServiceFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }
                MongoTrendsOutput serviceMongoOut = new MongoTrendsOutput(dbURI, "flipflop_trends_services", MongoTrendsOutput.TrendsType.TRENDS_SERVICE, reportID, runDate, clearMongo);

                trends = noZeroServiceFlipFlops.map(new MapServiceTrends());

                trends.output(serviceMongoOut);

                DataSet<GroupTrends> noZeroGroupFlipFlops = groupTrends.filter(new ZeroGroupFlipFlopFilter());

                if (rankNum != null) { //sort and rank data
                    noZeroGroupFlipFlops = noZeroGroupFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
                } else {
                    noZeroGroupFlipFlops = noZeroGroupFlipFlops.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
                }
                MongoTrendsOutput groupMongoOut = new MongoTrendsOutput(dbURI, "flipflop_trends_endpoint_groups", MongoTrendsOutput.TrendsType.TRENDS_GROUP, reportID, runDate, clearMongo);
                trends = noZeroGroupFlipFlops.map(new MapGroupTrends());

                trends.output(groupMongoOut);

            }

            if (calcStatusTrends) {
                //flatMap dataset to tuples and count the apperances of each status type to the timeline 
                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> metricStatusTrendsData = metricTrends.flatMap(new MetricTrendsCounter()).withBroadcastSet(opsDS, "ops").withBroadcastSet(mtagsDS, "mtags");
                //filter dataset for each status type and write to mongo db
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_METRIC, "status_trends_metrics", metricStatusTrendsData, "critical");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_METRIC, "status_trends_metrics", metricStatusTrendsData, "warning");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_METRIC, "status_trends_metrics", metricStatusTrendsData, "unknown");

                /*=============================================================================================*/
                //flatMap dataset to tuples and count the apperances of each status type to the timeline 
                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> endpointStatusTrendsData = endpointTrends.flatMap(new EndpointTrendsCounter()).withBroadcastSet(opsDS, "ops");
                //filter dataset for each status type and write to mongo db

                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT, "status_trends_endpoints", endpointStatusTrendsData, "critical");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT, "status_trends_endpoints", endpointStatusTrendsData, "warning");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT, "status_trends_endpoints", endpointStatusTrendsData, "unknown");

                /**
                 * **************************************************************************************************
                 */
                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> serviceStatusTrendsData = serviceTrends.flatMap(new ServiceTrendsCounter()).withBroadcastSet(opsDS, "ops");
                //filter dataset for each status type and write to mongo db
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_SERVICE, "status_trends_services", serviceStatusTrendsData, "critical");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_SERVICE, "status_trends_services", serviceStatusTrendsData, "warning");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_SERVICE, "status_trends_services", serviceStatusTrendsData, "unknown");

                /**
                 * ********************************************************************************************
                 */
                //group data by group   and count flip flops
                //flatMap dataset to tuples and count the apperances of each status type to the timeline 
                DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> groupStatusTrendsData = groupTrends.flatMap(new GroupTrendsCounter()).withBroadcastSet(opsDS, "ops");
                //filter dataset for each status type and write to mongo db
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_GROUP, "status_trends_groups", groupStatusTrendsData, "critical");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_GROUP, "status_trends_groups", groupStatusTrendsData, "warning");
                filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_GROUP, "status_trends_groups", groupStatusTrendsData, "unknown");
            }

        }
        // Create a job title message to discern job in flink dashboard/cli
        StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Multi Batch job for tenant:");
        jobTitleSB.append(cfgMgr.getTenant());
        jobTitleSB.append(" on day:");
        jobTitleSB.append(runDate);
        jobTitleSB.append(" using report:");
        jobTitleSB.append(cfgMgr.getReport());

        env.execute(jobTitleSB.toString());

    }

    private static void filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType mongoTrendsType, String uri, DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> data, String status) {

        DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> filteredData = data.filter(new StatusAndDurationFilter(status)); //filter dataset by status type and status appearances>0

        if (rankNum != null) {
            filteredData = filteredData.sortPartition(7, Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            filteredData = filteredData.sortPartition(7, Order.DESCENDING).setParallelism(1);
        }
        writeStatusTrends(filteredData, uri, mongoTrendsType, data, status); //write to mongo db

    }

    // write status trends to mongo db
    private static void writeStatusTrends(DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> outputData, String uri, final MongoTrendsOutput.TrendsType mongoCase, DataSet<Tuple8<String, String, String, String, String, Integer, Integer, String>> data, String status) {

        //MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT
        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(dbURI, uri, mongoCase, reportID, runDate, clearMongo);

        DataSet<Trends> trends = outputData.map(new MapFunction<Tuple8<String, String, String, String, String, Integer, Integer, String>, Trends>() {

            @Override
            public Trends map(Tuple8<String, String, String, String, String, Integer, Integer, String> in) throws Exception {
                switch (mongoCase) {
                    case TRENDS_STATUS_METRIC:
                        return new Trends(in.f0, in.f1, in.f2, in.f3, in.f4, in.f5, in.f6, in.f7);
                    case TRENDS_STATUS_ENDPOINT:
                        return new Trends(in.f0, in.f1, in.f2, null, in.f4, in.f5, in.f6, null);
                    case TRENDS_STATUS_SERVICE:
                        return new Trends(in.f0, in.f1, null, null, in.f4, in.f5, in.f6, null);
                    case TRENDS_STATUS_GROUP:
                        return new Trends(in.f0, null, null, null, in.f4, in.f5, in.f6, null);
                    default:
                        return null;
                }
            }
        });
        trends.output(metricMongoOut);

        //   writeToMongo(collectionUri, filteredData);
    }

    private static void enableComputations(ReportManager.ActiveComputations activeComputations, ParameterTool params) {

        calcStatus = isOFF(params, "calcStatus", activeComputations);
        calcAR = isOFF(params, "calcAR", activeComputations);
        calcStatusTrends = isOFF(params, "calcStatusTrends", activeComputations);
        calcFlipFlops = isOFF(params, "calcFlipFlops", activeComputations);
        calcTagTrends = isOFF(params, "calcTagTrends", activeComputations);

        if (!calcStatus && !calcAR && !calcStatusTrends && !calcFlipFlops && !calcStatusTrends) {
            System.exit(0);
        }

    }

    public static boolean isOFF(ParameterTool params, String paramName, ReportManager.ActiveComputations activeComputations) {
        if (params.has(paramName)) {
            return !params.get(paramName).equals("OFF");
        } else {
            switch (paramName) {
                case "calcStatus":
                    return activeComputations.isStatus();
                case "calcAR":
                    return activeComputations.isAr();

                case "calcStatusTrends":
                    return activeComputations.isStatusTrends();
                case "calcFlipFlops":
                    return activeComputations.isFlipflops();

                case "calcTagTrends":
                    return activeComputations.isTagTrends();
                default:
                    return false;
            }
        }
    }

    private static String getJID() {
        return JobID.generate().toString();
    }

    private static void configJID() {//config the JID in the log4j.properties
        String jobId = getJID();
        MDC.put("JID", jobId);

    }
}
