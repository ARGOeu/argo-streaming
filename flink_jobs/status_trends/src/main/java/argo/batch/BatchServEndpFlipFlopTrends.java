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
import argo.functions.calctrends.CalcEndpointFlipFlopTrends;
import argo.functions.calctimelines.CalcLastTimeStatus;
import argo.functions.calctimelines.TopologyMetricFilter;
import argo.pojos.EndpointTrends;
import argo.pojos.MetricTrends;
import argo.profiles.OperationsParser;
import argo.utils.Utils;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import java.util.ArrayList;

import java.util.HashMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import argo.profiles.ProfilesLoader;
import org.apache.flink.configuration.Configuration;

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
public class BatchServEndpFlipFlopTrends {

    private static HashMap<String, HashMap<String, String>> opTruthTableMap = new HashMap<>(); // the truth table for the operations to be applied on timeline
    private static HashMap<String, ArrayList<String>> metricProfileData;
    private static HashMap<String, String> groupEndpointData;
    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;
    private static final String endpointTrends = "endpointTrends";
    private static String mongoUri;
    private static ProfilesLoader profilesLoader;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

         ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "mongoUri","apiUri", "key")) {
            System.exit(0);
        }

        env.setParallelism(1);
        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        mongoUri = params.get("mongoUri");
         profilesLoader = new ProfilesLoader(params);
        metricProfileData = profilesLoader.getMetricProfileParser().getMetricData();
        groupEndpointData = profilesLoader.getTopologyEndpointParser().getTopology(profilesLoader.getAggregationProfileParser().getEndpointGroup().toUpperCase());

        yesterdayData = readInputData(env, params, "yesterdayData");
        todayData = readInputData(env, params, "todayData");
       
        Configuration conf = new Configuration();
        conf.setClass("opParser", OperationsParser.class);
        env.getConfig().setGlobalJobParameters(conf);
        // calculate on data 
        DataSet<EndpointTrends> resultData = calcFlipFlops();
        writeToMongo(resultData);

// execute program
        env.execute("Flink Batch Java API Skeleton");

    }

// filter yesterdaydata and exclude the ones not contained in topology and metric profile data and get the last timestamp data for each service endpoint metric
// filter todaydata and exclude the ones not contained in topology and metric profile data , union yesterday data and calculate status changes for each service endpoint metric
// rank results
    private static DataSet<EndpointTrends> calcFlipFlops() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(metricProfileData, groupEndpointData)).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());
        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(metricProfileData, groupEndpointData));

        //group data by service enpoint metric and return for each group , the necessary info and a treemap containing timestamps and status
        DataSet<MetricTrends> serviceEndpointMetricGroupData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcMetricFlipFlopTrends(groupEndpointData));

        //group data by service endpoint  and count flip flops
        DataSet<EndpointTrends> serviceEndpointGroupData = serviceEndpointMetricGroupData.groupBy("group", "endpoint", "service").reduceGroup(new CalcEndpointFlipFlopTrends(profilesLoader.getAggregationProfileParser().getMetricOp(), profilesLoader.getOperationParser()));

        if (rankNum != null) { //sort and rank data
            serviceEndpointGroupData = serviceEndpointGroupData.sortPartition("flipflops", Order.DESCENDING).first(rankNum);
        } else {
            serviceEndpointGroupData = serviceEndpointGroupData.sortPartition("flipflops", Order.DESCENDING);
        }
        return serviceEndpointGroupData;

    }    //read input from file

    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, ParameterTool params, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(params.getRequired(path));

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class
        );
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }

////    //initialize configuaration parameters to be used from functions
  //  private static void initializeConfigurationParameters(ParameterTool params, ExecutionEnvironment env) {
//
//        Configuration conf = new Configuration();
//        conf.setClass("opParser", OperationsParser.class);
//      
//        env.getConfig().setGlobalJobParameters(conf);
//
  // }

    //convert the result in bson format
    public static DataSet<Tuple2<Text, BSONWritable>> convertResultToBSON(DataSet<EndpointTrends> in) {

        return in.map(new MapFunction<EndpointTrends, Tuple2<Text, BSONWritable>>() {
            int i = 0;

            @Override
            public Tuple2<Text, BSONWritable> map(EndpointTrends in) throws Exception {
                BasicDBObject dbObject = new BasicDBObject();
                dbObject.put("group", in.getGroup().toString());
                dbObject.put("service", in.getService().toString());
                dbObject.put("hostname", in.getEndpoint().toString());
                dbObject.put("trend", in.getFlipflops());

                BSONWritable bson = new BSONWritable(dbObject);
                i++;
                return new Tuple2<Text, BSONWritable>(new Text(String.valueOf(i)), bson);
                /* TODO */
            }
        });
    }

    //write to mongo db
    public static void writeToMongo(DataSet<EndpointTrends> data) {
        String collectionUri = mongoUri + "." + endpointTrends;
        DataSet<Tuple2<Text, BSONWritable>> result = convertResultToBSON(data);
        JobConf conf = new JobConf();
        conf.set("mongo.output.uri", collectionUri);

        MongoOutputFormat<Text, BSONWritable> mongoOutputFormat = new MongoOutputFormat<Text, BSONWritable>();
        result.output(new HadoopOutputFormat<Text, BSONWritable>(mongoOutputFormat, conf));
    }

//    public static void createOpTruthTables(String baseUri, String key, String proxy, String operationsId) throws IOException, ParseException {
//        
//        opTruthTableMap = Utils.readOperationProfileJson(baseUri, key, proxy, operationsId);
//    }
}
