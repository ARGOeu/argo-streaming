/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.batch;

import argo.avro.MetricData;
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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;

/**
 *
 * @author cthermolia
 */
public class BatchFlipFlopsCollection {

    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;
    private static final String groupTrends = "flipflop_trends_endpoint_groups";
    private static final String metricTrends = "flipflop_trends_metrics";
    private static final String endpointTrends = "flipflop_trends_endpoints";
    private static final String serviceTrends = "flipflop_trends_services";
    private static String mongoUri;
    private static ProfilesLoader profilesLoader;
    private static String profilesDate;
    private static String format = "yyyy-MM-dd";
    private static String reportId;
    private static boolean clearMongo = false;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "mongoUri", "apiUri", "key", "date", "reportId")) {
            System.exit(0);
        }

        env.setParallelism(1);
        if (params.get("clearMongo") != null && params.getBoolean("clearMongo") == true) {
            clearMongo = true;
        }
        reportId = params.getRequired("reportId");
        profilesDate = Utils.getParameterDate(format, params.getRequired("date"));
        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        mongoUri = params.get("mongoUri");
        profilesLoader = new ProfilesLoader(params);
        yesterdayData = readInputData(env, params, "yesterdayData");
        todayData = readInputData(env, params, "todayData");

        // calculate on data 
        //DataSet<GroupTrends> resultData = 
        calcFlipFlops();
        //writeToMongo(resultData);

// execute program
        env.execute("Flink Batch Java API Skeleton");

    }

// filter yesterdaydata and exclude the ones not contained in topology and metric profile data and get the last timestamp data for each service endpoint metric
// filter todaydata and exclude the ones not contained in topology and metric profile data , union yesterday data and calculate status changes for each service endpoint metric
// rank results
//    private static DataSet<GroupTrends> calcFlipFlops() {
    private static void calcFlipFlops() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser())).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());
        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser()));

        //group data by service enpoint metric and return for each group , the necessary info and a treemap containing timestamps and status
        DataSet<MetricTrends> serviceEndpointMetricGroupData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcMetricFlipFlopTrends(profilesLoader.getTopologyEndpointParser(), profilesLoader.getAggregationProfileParser()));
        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, metricTrends, MongoTrendsOutput.TrendsType.TRENDS_METRIC, reportId, profilesDate, clearMongo);

        DataSet<Trends> trends = serviceEndpointMetricGroupData.map(new MapFunction<MetricTrends, Trends>() {

            @Override
            public Trends map(MetricTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getMetric(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

        //group data by service endpoint  and count flip flops
        DataSet<EndpointTrends> serviceEndpointGroupData = serviceEndpointMetricGroupData.groupBy("group", "endpoint", "service").reduceGroup(new CalcEndpointFlipFlopTrends(profilesLoader.getAggregationProfileParser().getMetricOp(), profilesLoader.getOperationParser()));

        metricMongoOut = new MongoTrendsOutput(mongoUri, endpointTrends, MongoTrendsOutput.TrendsType.TRENDS_ENDPOINT, reportId, profilesDate, clearMongo);

        trends = serviceEndpointGroupData.map(new MapFunction<EndpointTrends, Trends>() {

            @Override
            public Trends map(EndpointTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

        //group data by service   and count flip flops
        DataSet<ServiceTrends> serviceGroupData = serviceEndpointGroupData.filter(new ServiceFilter(profilesLoader.getAggregationProfileParser())).groupBy("group", "service").reduceGroup(new CalcServiceFlipFlop(profilesLoader.getOperationParser(), profilesLoader.getAggregationProfileParser()));
        metricMongoOut = new MongoTrendsOutput(mongoUri, serviceTrends, MongoTrendsOutput.TrendsType.TRENDS_SERVICE, reportId, profilesDate, clearMongo);

        trends = serviceGroupData.map(new MapFunction<ServiceTrends, Trends>() {

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

        //group data by group   and count flip flops
        DataSet<GroupTrends> groupData = groupFunction.groupBy("group").reduceGroup(new CalcGroupFlipFlop(profilesLoader.getOperationParser(), profilesLoader.getAggregationProfileParser()));

        if (rankNum != null) { //sort and rank data
            groupData = groupData.sortPartition("flipflops", Order.DESCENDING).first(rankNum);
        } else {
            groupData = groupData.sortPartition("flipflops", Order.DESCENDING);
        }

        metricMongoOut = new MongoTrendsOutput(mongoUri, groupTrends, MongoTrendsOutput.TrendsType.TRENDS_GROUP, reportId, profilesDate, clearMongo);

        trends = groupData.map(new MapFunction<GroupTrends, Trends>() {

            @Override
            public Trends map(GroupTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);
        //  return groupData;

    }    //read input from file

    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, ParameterTool params, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(params.getRequired(path));

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class
        );
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }
}
