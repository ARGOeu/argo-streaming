package argo.batch;

import argo.avro.MetricData;
import argo.functions.calctrends.CalcMetricFlipFlopTrends;
import argo.functions.calctimelines.CalcLastTimeStatus;
import argo.functions.calctimelines.TopologyMetricFilter;
import argo.pojos.MetricTrends;
import argo.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import argo.profiles.ProfilesLoader;
import org.joda.time.DateTime;

/**
 * Implements an ARGO Status Trends Job in flink , to count the number of status
 * changes that occur to the level of group, service, endpoint, metric of the
 * topology hierarchy
 *
 * Submit job in flink cluster using the following parameters  *
 * --date:the date for which the job runs and need to return results ,
 * yyyy-MM-dd --yesterdayData: path to the metric profile data, of the previous
 * day , for which the jobs runs profile (For hdfs use:
 * hdfs://namenode:port/path/to/file) --todayData: path to the metric profile
 * data, of the current day , for which the jobs runs profile (For hdfs use:
 * hdfs://namenode:port/path/to/file) --mongoUri: path to MongoDB destination
 * (eg mongodb://localhost:27017/database --apiUri: path to the mongo db the ,
 * for which the jobs runs profile (For hdfs use:
 * hdfs://namenode:port/path/to/file) --key: ARGO web api token --reportId: the
 * id of the report the job will need to process --apiUri: ARGO wep api to
 * connect to msg.example.com Optional: -- clearMongo: option to clear the mongo
 * db before saving the new result or not, e.g true -- N : the number of the
 * result the job will provide, if the parameter exists , e.g 10
 */
public class BatchMetricFlipFlopTrends {

    static Logger LOG = LoggerFactory.getLogger(BatchMetricFlipFlopTrends.class);

    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;
    private static final String metricTrends = "flipflop_trends_metrics";
    private static String mongoUri;

    private static DateTime profilesDate;
    private static String profilesDateStr;
    private static String reportId;
    private static final String format = "yyyy-MM-dd";
    private static ProfilesLoader profilesLoader;

    private static boolean clearMongo = false;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "mongoUri", "apiUri", "key", "reportId", "date")) {
            System.exit(0);
        }

        if (params.get("clearMongo") != null && params.getBoolean("clearMongo") == true) {
            clearMongo = true;
        }

        profilesDate = Utils.convertStringtoDate(format, params.getRequired("date"));
        profilesDateStr = Utils.convertDateToString(format, profilesDate);
        mongoUri = params.getRequired("mongoUri");

        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        reportId = params.getRequired("reportId");

        profilesLoader = new ProfilesLoader(params);
        yesterdayData = readInputData(env, params.getRequired("yesterdayData"));
        todayData = readInputData(env, params.getRequired("todayData"));

        calcFlipFlops();

// execute program
        StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Metric Flip Flops for: ");
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
//    private static DataSet<MetricTrends> calcFlipFlops() {
    private static void calcFlipFlops() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser())).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());

        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser()));

        DataSet<MetricTrends> metricData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcMetricFlipFlopTrends(profilesLoader.getOperationParser(), profilesLoader.getTopologyEndpointParser(),profilesLoader.getTopolGroupParser(),profilesLoader.getAggregationProfileParser(), profilesDate));

        if (rankNum != null) {
            metricData = metricData.sortPartition("flipflops", Order.DESCENDING).first(rankNum);

        } else {
            metricData = metricData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);

        }
      
        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, metricTrends, MongoTrendsOutput.TrendsType.TRENDS_METRIC, reportId, profilesDateStr, clearMongo);
        DataSet<Trends> trends = metricData.map(new MapFunction<MetricTrends, Trends>() {

            @Override
            public Trends map(MetricTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getMetric(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

    }
    //read input from file

    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(path);

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class);
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }

}
