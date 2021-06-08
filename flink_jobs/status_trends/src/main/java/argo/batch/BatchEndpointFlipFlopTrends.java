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
import argo.functions.calctimelines.CalcLastTimeStatus;
import argo.functions.calctimelines.TopologyMetricFilter;
import argo.functions.calctrends.CalcEndpointFlipFlopTrends;
import argo.functions.calctrends.CalcMetricFlipFlopTrends;
import argo.pojos.EndpointTrends;
import argo.pojos.MetricTrends;
import argo.utils.Utils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import argo.profiles.ProfilesLoader;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.core.fs.Path;
import org.joda.time.DateTime;

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
public class BatchEndpointFlipFlopTrends {

    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;
     private static final String endpointTrends = "flipflop_trends_endpoints";

    private static String mongoUri;
    private static ProfilesLoader profilesLoader;
    private static DateTime profilesDate;

    private static String reportId;
    private static String format = "yyyy-MM-dd";
    private static boolean clearMongo = false;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program

        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "mongoUri", "apiUri", "key", "reportId", "date")) {
            System.exit(0);

        }

        if (params.get("clearMongo") != null && params.getBoolean("clearMongo") == true) {
            clearMongo = true;
        }
        String profilesDateStr = Utils.getParameterDate(format, params.getRequired("date"));
        profilesDate = Utils.convertStringtoDate(format, profilesDateStr);

        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        reportId = params.getRequired("reportId");
        mongoUri = params.getRequired("mongoUri");
        profilesLoader = new ProfilesLoader(params);
        yesterdayData = readInputData(env, params, "yesterdayData");
        todayData = readInputData(env, params, "todayData");

        calcFlipFlops();
// execute program
       StringBuilder jobTitleSB = new StringBuilder();
        jobTitleSB.append("Group Endpoint Flip Flops for: ");
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
    //  private static DataSet<EndpointTrends> calcFlipFlops() {
    private static void calcFlipFlops() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser())).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());
        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(profilesLoader.getMetricProfileParser(), profilesLoader.getTopologyEndpointParser(), profilesLoader.getTopolGroupParser(), profilesLoader.getAggregationProfileParser()));

        //group data by service enpoint metric and return for each group , the necessary info and a treemap containing timestamps and status

        DataSet<MetricTrends> serviceEndpointMetricGroupData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcMetricFlipFlopTrends(profilesLoader.getOperationParser(),profilesLoader.getTopologyEndpointParser(), profilesLoader.getAggregationProfileParser(), profilesDate));

        //group data by service endpoint  and count flip flops
        DataSet<EndpointTrends> serviceEndpointGroupData = serviceEndpointMetricGroupData.groupBy("group", "endpoint", "service").reduceGroup(new CalcEndpointFlipFlopTrends(profilesLoader.getAggregationProfileParser().getMetricOp(), profilesLoader.getOperationParser()));

        if (rankNum != null) { //sort and rank data
            serviceEndpointGroupData = serviceEndpointGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1).first(rankNum);
        } else {
            serviceEndpointGroupData = serviceEndpointGroupData.sortPartition("flipflops", Order.DESCENDING).setParallelism(1);
        }
        MongoTrendsOutput metricMongoOut = new MongoTrendsOutput(mongoUri, endpointTrends, MongoTrendsOutput.TrendsType.TRENDS_METRIC, reportId, profilesDate.toString(), clearMongo);

        DataSet<Trends> trends = serviceEndpointGroupData.map(new MapFunction<EndpointTrends, Trends>() {

            @Override
            public Trends map(EndpointTrends in) throws Exception {
                return new Trends(in.getGroup(), in.getService(), in.getEndpoint(), in.getFlipflops());
            }
        });
        trends.output(metricMongoOut);

        // return serviceEndpointGroupData;
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
