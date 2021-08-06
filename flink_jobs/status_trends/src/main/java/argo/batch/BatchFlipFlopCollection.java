package argo.batch;

import argo.avro.MetricData;
import argo.filter.zero.flipflops.ZeroEndpointTrendsFilter;
import argo.filter.zero.flipflops.ZeroGroupTrendsFilter;
import argo.filter.zero.flipflops.ZeroMetricTrendsFilter;
import argo.filter.zero.flipflops.ZeroServiceTrendsFilter;
import argo.functions.calctimelines.CalcLastTimeStatus;
import argo.functions.calctimelines.MapServices;
import argo.functions.calctimelines.ServiceFilter;
import argo.functions.calctimelines.TopologyMetricFilter;
import argo.functions.calctrends.CalcEndpointFlipFlopTrends;
import argo.functions.calctrends.CalcGroupFlipFlop;
import argo.functions.calctrends.CalcGroupFunctionFlipFlop;
import argo.functions.calctrends.CalcMetricFlipFlopTrends;
import argo.functions.calctrends.CalcServiceFlipFlop;
import argo.pojos.EndpointTrends;
import argo.pojos.GroupFunctionTrends;
import argo.pojos.GroupTrends;
import argo.pojos.MetricTrends;
import argo.pojos.ServiceTrends;
import argo.profiles.ProfilesLoader;
import argo.utils.Utils;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.joda.time.DateTime;
/**
 * Implements an ARGO Status Trends Job in flink , to count the number of status changes
 * that occur to all levels of the topology hierarchy
 * 
 * Submit job in flink cluster using the following parameters 

* --date:the date for which the job runs and need to return results , e.g yyyy-MM-dd
* --yesterdayData: path to the metric profile data, of the previous day , for which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
* --todayData: path to the metric profile data, of the current day , for which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
* --mongoUri: path to MongoDB destination (eg mongodb://localhost:27017/database
* --apiUri: path to the mongo db the  , for which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
* --key: ARGO web api token    
* --reportId: the id of the report the job will need to process
*  --apiUri: ARGO wep api to connect to e.g msg.example.com
* Optional: 
* -- clearMongo: option to clear the mongo db before saving the new result or not, e.g  true 
* -- N : the number of the result the job will provide, if the parameter exists , e.g 10
* 
*/

public class BatchFlipFlopCollection {

    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;
    private static final String groupTrends = "flipflop_trends_endpoint_groups";
    private static final String metricTrends = "flipflop_trends_metrics";
    private static final String endpointTrends = "flipflop_trends_endpoints";
    private static final String serviceTrends = "flipflop_trends_services";
    private static String mongoUri;
    private static ProfilesLoader profilesLoader;
    private static DateTime profilesDate;
    private static String format = "yyyy-MM-dd";
    private static String reportId;
    private static boolean clearMongo = false;
    private static String profilesDateStr;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.addDefaultKryoSerializer(DateTime.class, JodaDateTimeSerializer.class);
        ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "mongoUri", "apiUri", "key", "date", "reportId")) {
            System.exit(0);
        }

        if (params.get("clearMongo") != null && params.getBoolean("clearMongo") == true) {
            clearMongo = true;
        }
        reportId = params.getRequired("reportId");
        profilesDate = Utils.convertStringtoDate(format, params.getRequired("date"));
        profilesDateStr = Utils.convertDateToString(format, profilesDate);

        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        mongoUri = params.get("mongoUri");
        profilesLoader = new ProfilesLoader(params);
        yesterdayData = readInputData(env, params, "yesterdayData");
        todayData = readInputData(env, params, "todayData");

        // calculate on data 
        calcFlipFlops();

// execute program
        StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Collection Flip Flops for: ");
        jobTitleSB.append(profilesLoader.getReportParser().getTenantReport().getTenant());
        jobTitleSB.append("/");
        jobTitleSB.append(profilesLoader.getReportParser().getTenantReport().getInfo()[0]);
        jobTitleSB.append("/");
        jobTitleSB.append(profilesDate);
        env.execute(jobTitleSB.toString());

    }

// filter yesterdaydata and exclude the ones not contained in topology and metric profile data and get the last timestamp data for each service endpoint metric
// filter todaydata and exclude the ones not contained in topology and metric profile data , union yesterday data and calculate status changes for each service endpoint metric
// rank results
//    private static DataSet<GroupTrends> calcFlipFlops() {
    private static void calcFlipFlops() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser())).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());
        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser()));

        //group data by service enpoint metric and return for each group , the necessary info and a treemap containing timestamps and status

        DataSet<MetricTrends> serviceEndpointMetricGroupData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcMetricFlipFlopTrends(profilesLoader.getOperationParser(), profilesLoader.getTopologyEndpointParser(),profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser(), profilesDate));

        DataSet<MetricTrends> noZeroServiceEndpointMetricGroupData = serviceEndpointMetricGroupData.filter(new ZeroMetricTrendsFilter());
        if (rankNum != null) { //sort and rank data
            noZeroServiceEndpointMetricGroupData = noZeroServiceEndpointMetricGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            noZeroServiceEndpointMetricGroupData = noZeroServiceEndpointMetricGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
        }

        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, metricTrends, MongoTrendsOutput.TrendsType.TRENDS_METRIC, reportId, profilesDateStr, clearMongo);
        DataSet<Trends> trends = noZeroServiceEndpointMetricGroupData.map(new MapFunction<MetricTrends, Trends>() {

            @Override
            public Trends map(MetricTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getMetric(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

        //group data by service endpoint  and count flip flops
        DataSet<EndpointTrends> serviceEndpointGroupData = serviceEndpointMetricGroupData.groupBy("group", "endpoint", "service").reduceGroup(new CalcEndpointFlipFlopTrends(profilesLoader.getAggregationProfileParser().getMetricOpByProfile(), profilesLoader.getOperationParser()));
        DataSet<EndpointTrends> noZeroserviceEndpointGroupData = serviceEndpointGroupData.filter(new ZeroEndpointTrendsFilter());

        if (rankNum != null) { //sort and rank data
            noZeroserviceEndpointGroupData = noZeroserviceEndpointGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            noZeroserviceEndpointGroupData = noZeroserviceEndpointGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
        }
        metricMongoOut = new MongoTrendsOutput(mongoUri, endpointTrends, MongoTrendsOutput.TrendsType.TRENDS_ENDPOINT, reportId, profilesDateStr, clearMongo);

        trends = noZeroserviceEndpointGroupData.map(new MapFunction<EndpointTrends, Trends>() {

            @Override
            public Trends map(EndpointTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

        //group data by service   and count flip flops
        DataSet<ServiceTrends> serviceGroupData = serviceEndpointGroupData.filter(new ServiceFilter(profilesLoader.getAggregationProfileParser())).groupBy("group", "service").reduceGroup(new CalcServiceFlipFlop(profilesLoader.getOperationParser(), profilesLoader.getAggregationProfileParser()));
        DataSet<ServiceTrends> noZeroserviceGroupData = serviceGroupData.filter(new ZeroServiceTrendsFilter());

        if (rankNum != null) { //sort and rank data
            noZeroserviceGroupData = noZeroserviceGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            noZeroserviceGroupData = noZeroserviceGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
        }
        metricMongoOut = new MongoTrendsOutput(mongoUri, serviceTrends, MongoTrendsOutput.TrendsType.TRENDS_SERVICE, reportId, profilesDateStr, clearMongo);

        trends = noZeroserviceGroupData.map(new MapFunction<ServiceTrends, Trends>() {

            @Override
            public Trends map(ServiceTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

        //flat map data to add function as described in aggregation profile groups
        serviceGroupData = serviceGroupData.flatMap(new MapServices(profilesLoader.getAggregationProfileParser()));

        //group data by group,function   and count flip flops
        DataSet<GroupFunctionTrends> groupFunction = serviceGroupData.groupBy("group", "function").reduceGroup(new CalcGroupFunctionFlipFlop(profilesLoader.getOperationParser(), profilesLoader.getAggregationProfileParser()));
        //  DataSet<GroupFunctionTrends> noZerogroupFunction =groupFunction.filter(new ZeroGroupFunctionFilter());

        //group data by group   and count flip flops
        DataSet<GroupTrends> groupData = groupFunction.groupBy("group").reduceGroup(new CalcGroupFlipFlop(profilesLoader.getOperationParser(), profilesLoader.getAggregationProfileParser()));
        DataSet<GroupTrends> noZerogroupData = groupData.filter(new ZeroGroupTrendsFilter());

        if (rankNum != null) { //sort and rank data
            noZerogroupData = noZerogroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            noZerogroupData = noZerogroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
        }
        metricMongoOut = new MongoTrendsOutput(mongoUri, groupTrends, MongoTrendsOutput.TrendsType.TRENDS_GROUP, reportId, profilesDateStr, clearMongo);
        trends = noZerogroupData.map(new MapFunction<GroupTrends, Trends>() {

            @Override
            public Trends map(GroupTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

    }

    //read input from file
    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, ParameterTool params, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(params.getRequired(path));

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class
        );
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }

}
