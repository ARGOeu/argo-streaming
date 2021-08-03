package argo.batch;

import argo.avro.MetricData;
import argo.filter.zero.flipflops.ZeroEndpointTrendsFilter;
import argo.filter.zero.flipflops.ZeroGroupTrendsFilter;
import argo.filter.zero.flipflops.ZeroMetricTrendsFilter;
import argo.filter.zero.flipflops.ZeroServiceTrendsFilter;
import argo.functions.calctimelines.CalcLastTimeStatus;
import argo.functions.calctimelines.MapServices;
import argo.functions.calctimelines.ServiceFilter;
import argo.functions.calctimelines.StatusAndDurationFilter;
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
import argo.status.trends.EndpointTrendsCounter;
import argo.status.trends.GroupTrendsCounter;
import argo.status.trends.MetricTrendsCounter;
import argo.status.trends.ServiceTrendsCounter;
import argo.utils.Utils;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.joda.time.DateTime;

/**
 * Implements an ARGO Trends Job in flink , to count the number of status
 * changes and also the num of appearances of the status CRITICAL,WARNING,UNKNOWN.
 * that occur to all levels of the topology hierarchy 
 * 
 *
 * Submit job in flink cluster using the following parameters * --date:the date
 * for which the job runs and need to return results , e.g yyyy-MM-dd
 * --yesterdayData: path to the metric profile data, of the previous day , for
 * which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
 * --todayData: path to the metric profile data, of the current day , for which
 * the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
 * --mongoUri: path to MongoDB destination (eg
 * mongodb://localhost:27017/database --apiUri: path to the mongo db the , for
 * which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
 * --key: ARGO web api token --reportId: the id of the report the job will need
 * to process --apiUri: ARGO wep api to connect to e.g msg.example.com Optional:
 * -- clearMongo: option to clear the mongo db before saving the new result or
 * not, e.g true -- N : the number of the result the job will provide, if the
 * parameter exists , e.g 10
 *
 */
public class BatchTrendsCalculations {

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
    private static final String statusTrendsMetricCol = "status_trends_metrics";
    private static final String statusTrendsEndpointCol = "status_trends_endpoints";
    private static final String statusTrendsServiceCol = "status_trends_services";
    private static final String statusTrendsGroupCol = "status_trends_groups";

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
        jobTitleSB.append("Calculation of Flip Flops & Status Trends for: ");
        jobTitleSB.append(profilesLoader.getReportParser().getTenant());
        jobTitleSB.append("/");
        jobTitleSB.append(profilesLoader.getReportParser().getReport());
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
        //flatMap dataset to tuples and count the apperances of each status type to the timeline 
        DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> metricStatusTrendsData = serviceEndpointMetricGroupData.flatMap(new MetricTrendsCounter(profilesLoader.getOperationParser()));
        //filter dataset for each status type and write to mongo db
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_METRIC, statusTrendsMetricCol, metricStatusTrendsData, "critical");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_METRIC, statusTrendsMetricCol, metricStatusTrendsData, "warning");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_METRIC, statusTrendsMetricCol, metricStatusTrendsData, "unknown");

        /**
         * *****************************************************************************************************************
         */
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
        //flatMap dataset to tuples and count the apperances of each status type to the timeline 
        DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> endpointStatusTrendsData = serviceEndpointGroupData.flatMap(new EndpointTrendsCounter(profilesLoader.getOperationParser()));
        //filter dataset for each status type and write to mongo db
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT, statusTrendsEndpointCol, endpointStatusTrendsData, "critical");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT, statusTrendsEndpointCol, endpointStatusTrendsData, "warning");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT, statusTrendsEndpointCol, endpointStatusTrendsData, "unknown");

        /**
         * **************************************************************************************************************************
         */
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
        //flatMap dataset to tuples and count the apperances of each status type to the timeline 
        DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> serviceStatusTrendsData = serviceGroupData.flatMap(new ServiceTrendsCounter(profilesLoader.getOperationParser()));
        //filter dataset for each status type and write to mongo db
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_SERVICE, statusTrendsServiceCol, serviceStatusTrendsData, "critical");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_SERVICE, statusTrendsServiceCol, serviceStatusTrendsData, "warning");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_SERVICE, statusTrendsServiceCol, serviceStatusTrendsData, "unknown");

        /**
         * ****************************************************************************************************
         */
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
        //flatMap dataset to tuples and count the apperances of each status type to the timeline 
        DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> groupStatusTrendsData = groupData.flatMap(new GroupTrendsCounter(profilesLoader.getOperationParser()));
        //filter dataset for each status type and write to mongo db
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_GROUP, statusTrendsGroupCol, groupStatusTrendsData, "critical");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_GROUP, statusTrendsGroupCol, groupStatusTrendsData, "warning");
        filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType.TRENDS_STATUS_GROUP, statusTrendsGroupCol, groupStatusTrendsData, "unknown");

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
 
    //filters the dataset to appear result by status type and status appearances>0 and write to mongo db
    private static void filterByStatusAndWriteMongo(MongoTrendsOutput.TrendsType mongoTrendsType, String uri, DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> data, String status) {

        DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> filteredData = data.filter(new StatusAndDurationFilter(status)); //filter dataset by status type and status appearances>0

        if (rankNum != null) {
            filteredData = filteredData.sortPartition(6, Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            filteredData = filteredData.sortPartition(6, Order.DESCENDING).setParallelism(1);
        }
        writeStatusTrends(filteredData, uri, mongoTrendsType, data, status); //write to mongo db
    }

    // write status trends to mongo db
    private static void writeStatusTrends(DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> outputData, String uri, final MongoTrendsOutput.TrendsType mongoCase, DataSet< Tuple7< String,String, String, String, String, Integer,Integer>> data, String status) {

        //MongoTrendsOutput.TrendsType.TRENDS_STATUS_ENDPOINT
        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, uri, mongoCase, reportId, profilesDateStr, clearMongo);

        DataSet<Trends> trends = outputData.map(new MapFunction< Tuple7< String,String, String, String, String, Integer,Integer>, Trends>() {

            @Override
            public Trends map( Tuple7< String,String, String, String, String, Integer,Integer> in) throws Exception {
                switch (mongoCase) {
                    case TRENDS_STATUS_METRIC:
                        return new Trends(in.f0, in.f1, in.f2, in.f3, in.f4, in.f5, in.f6);
                    case TRENDS_STATUS_ENDPOINT:
                        return new Trends(in.f0, in.f1, in.f2, null, in.f4, in.f5, in.f6);
                    case TRENDS_STATUS_SERVICE:
                        return new Trends(in.f0, in.f1, null, null, in.f4, in.f5, in.f6);
                    case TRENDS_STATUS_GROUP:
                        return new Trends(in.f0, null, null, null, in.f4, in.f5, in.f6);
                    default:
                        return null;
                }
            }
        });
        trends.output(metricMongoOut);

        //   writeToMongo(collectionUri, filteredData);
    }

}
