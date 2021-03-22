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
public class BatchMetricFlipFlopTrends {

    static Logger LOG = LoggerFactory.getLogger(BatchMetricFlipFlopTrends.class);

    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;
    private static final String metricTrends = "flipflop_trends_metrics";
    private static String mongoUri;
    private static ProfilesLoader profilesLoader;
    private static String profilesDate;
    private static final String format = "yyyy-MM-dd";
    private static boolean clearMongo = false;
    private static String reportId;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "mongoUri", "apiUri", "key", "reportId", "date")) {
            System.exit(0);
        }
        env.setParallelism(1);

        if (params.get("clearMongo") != null && params.getBoolean("clearMongo") == true) {
            clearMongo = true;
        }
        reportId = params.getRequired("reportId");

        profilesDate = Utils.getParameterDate(format, params.getRequired("date"));

        profilesDate = Utils.getParameterDate(format, params.getRequired("date"));
        mongoUri = params.getRequired("mongoUri");
        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }

        profilesLoader = new ProfilesLoader(params);
        yesterdayData = readInputData(env, params.getRequired("yesterdayData"));
        todayData = readInputData(env, params.getRequired("todayData"));

        calcFlipFlops();

// execute program
        env.execute("Flink Batch Java API Skeleton");
    }

    // filter yesterdaydata and exclude the ones not contained in topology and metric profile data and get the last timestamp data for each service endpoint metric
    // filter todaydata and exclude the ones not contained in topology and metric profile data , union yesterday data and calculate status changes for each service endpoint metric
    // rank results
    private static void calcFlipFlops() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser())).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());

        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser()));
        DataSet<MetricTrends> metricData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcMetricFlipFlopTrends(profilesLoader.getTopologyEndpointParser(), profilesLoader.getAggregationProfileParser()));
        if (rankNum != null) {
            metricData = metricData.sortPartition("flipflops", Order.DESCENDING).first(rankNum);
        } else {
            metricData = metricData.sortPartition("flipflops", Order.DESCENDING);

        }
        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, metricTrends, MongoTrendsOutput.TrendsType.TRENDS_METRIC, reportId, profilesDate, clearMongo);
        DataSet<Trends> trends = metricData.map(new MapFunction<MetricTrends, Trends>() {

            @Override
            public Trends map(MetricTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getMetric(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);
        // return reducedData;
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
