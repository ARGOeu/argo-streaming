package argo.batch;

import argo.avro.MetricData;
import argo.functions.calctrends.CalcStatusTrends;
import argo.functions.calctimelines.TopologyMetricFilter;
import argo.functions.calctimelines.CalcLastTimeStatus;
import argo.functions.calctimelines.StatusFilter;
import argo.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import argo.profiles.ProfilesLoader;
import org.joda.time.DateTime;

/**
 * Implements an ARGO Status Trends Job in flink , to count the number of status appearances per status type
 * that occur to the level of group, service, endpoint. metric  of the topology hierarchy
 * 
 * Submit job in flink cluster using the following parameters 

* --date:the date for which the job runs and need to return results , yyyy-MM-dd
* --yesterdayData: path to the metric profile data, of the previous day , for which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
* --todayData: path to the metric profile data, of the current day , for which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
* --mongoUri: path to MongoDB destination (eg mongodb://localhost:27017/database
* --apiUri: path to the mongo db the  , for which the jobs runs profile (For hdfs use: hdfs://namenode:port/path/to/file)
* --key: ARGO web api token    
* --reportId: the id of the report the job will need to process
*  --apiUri: ARGO wep api to connect to msg.example.com
*Optional: 
* -- clearMongo: option to clear the mongo db before saving the new result or not, e.g  true 
* -- N : the number of the result the job will provide, if the parameter exists , e.g 10
* 
*/
public class BatchStatusTrends {

    static Logger LOG = LoggerFactory.getLogger(BatchStatusTrends.class);

    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;

    private static final String statusTrendsCol = "status_trends_metrics";
   

    private static String mongoUri;
    private static ProfilesLoader profilesLoader;

    private static String reportId;
    private static DateTime profilesDate;
    private static String format = "yyyy-MM-dd";

    private static boolean clearMongo = false;
    private static String profilesDateStr;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "apiUri", "key", "date", "reportId")) {
            System.exit(0);
        }

     
        if (params.get("clearMongo") != null && params.getBoolean("clearMongo") == true) {
            clearMongo = true;
        }
        profilesDate = Utils.convertStringtoDate(format, params.getRequired("date"));
        profilesDateStr = Utils.convertDateToString(format, profilesDate);
        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        reportId = params.getRequired("reportId");
        mongoUri = params.get("mongoUri");

        profilesLoader = new ProfilesLoader(params);
        yesterdayData = readInputData(env, params, "yesterdayData");
        todayData = readInputData(env, params, "todayData");

        DataSet<Tuple6<String, String, String, String, String, Integer>> rankedData = rankByStatus();
        filterByStatusAndWrite(statusTrendsCol, rankedData, "critical");
        filterByStatusAndWrite(statusTrendsCol, rankedData, "warning");
        filterByStatusAndWrite(statusTrendsCol, rankedData, "unknown");

// execute program
             StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Status Trends for: ");
        jobTitleSB.append(profilesLoader.getReportParser().getTenantReport().getTenant());
        jobTitleSB.append("/");
        jobTitleSB.append(profilesLoader.getReportParser().getTenantReport().getInfo()[0]);
        jobTitleSB.append("/");
        jobTitleSB.append(profilesDate);
        env.execute(jobTitleSB.toString());
    }

    //filters the yesterdayData and exclude the ones not in topology and metric profile data and keeps the last timestamp for each service endpoint metric
    //filters the todayData and exclude the ones not in topology and metric profile data, union with yesterdayData and calculates the times each status (CRITICAL,WARNING.UNKNOW) appears
    private static DataSet<Tuple6<String, String, String, String, String, Integer>> rankByStatus() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser())).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());

        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser()));
        DataSet<Tuple6<String, String, String, String, String, Integer>> rankedData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcStatusTrends(profilesLoader.getTopologyEndpointParser(), profilesLoader.getAggregationProfileParser()));

        return rankedData;
    }

    // filter the data based on status (CRITICAL,WARNING,UNKNOWN), rank and write top N in seperate files for each status
    private static void filterByStatusAndWrite(String uri, DataSet<Tuple6<String, String, String, String, String, Integer>> data, String status) {
        String collectionUri = mongoUri + "." + uri;
        DataSet<Tuple6<String, String, String, String, String, Integer>> filteredData = data.filter(new StatusFilter(status));

        if (rankNum != null) {
            filteredData = filteredData.sortPartition(5, Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            filteredData = filteredData.sortPartition(5, Order.DESCENDING).setParallelism(1);
        }

        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, uri, MongoTrendsOutput.TrendsType.TRENDS_STATUS, reportId, profilesDateStr, clearMongo);

        DataSet<Trends> trends = filteredData.map(new MapFunction<Tuple6<String, String, String, String, String, Integer>, Trends>() {

            @Override
            public Trends map(Tuple6<String, String, String, String, String, Integer> in) throws Exception {
                return new Trends(in.f0.toString(), in.f1.toString(), in.f2.toString(), in.f3.toString(), in.f4.toString(), in.f5);
            }
        });
        trends.output(metricMongoOut);

        //   writeToMongo(collectionUri, filteredData);
    }

    // reads input from file
    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, ParameterTool params, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(params.getRequired(path));

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class);
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }

}
