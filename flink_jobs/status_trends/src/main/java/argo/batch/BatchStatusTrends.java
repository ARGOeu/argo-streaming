package argo.batch;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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

/**
 * Skeleton for a Flink Batch Job.
 *
 * For a full example of a Flink Batch Job, see the WordCountJob.java file in
 * the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink cluster.
 * Just type mvn clean package in the projects root directory. You will find the
 * jar in target/argo2932-1.0.jar From the CLI you can then run ./bin/flink run
 * -c com.company.argo.BatchJob target/argo2932-1.0.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class BatchStatusTrends {

    static Logger LOG = LoggerFactory.getLogger(BatchStatusTrends.class);
    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;

    private static final String criticalStatusTrends = "status_trends_critical";
    private static final String warningStatusTrends = "status_trends_warning";
    private static final String unknownStatusTrends = "status_trends_unknown";

    private static String mongoUri;
    private static ProfilesLoader profilesLoader;
    private static String profilesDate;
    private static String format = "yyyy-MM-dd";
    private static String reportId;
    private static boolean clearMongo = false;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "apiUri", "key", "date")) {
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

        DataSet<Tuple6<String, String, String, String, String, Integer>> rankedData = rankByStatus();
        filterByStatusAndWrite(criticalStatusTrends, rankedData, "critical");
        filterByStatusAndWrite(warningStatusTrends, rankedData, "warning");
        filterByStatusAndWrite(unknownStatusTrends, rankedData, "unknown");

// execute program
        env.execute("Flink Batch Java API Skeleton");
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
            filteredData = filteredData.sortPartition(5, Order.DESCENDING).first(rankNum);
        } else {
            filteredData = filteredData.sortPartition(5, Order.DESCENDING);
        }

        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, uri, MongoTrendsOutput.TrendsType.TRENDS_STATUS, reportId, profilesDate, clearMongo);

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
