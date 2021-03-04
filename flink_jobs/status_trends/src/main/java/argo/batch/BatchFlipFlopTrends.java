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
import argo.functions.flipfloptrends.CalcServiceEnpointMetricFlipFlop;
import argo.functions.timeline.CalcLastTimeStatus;

import argo.functions.timeline.TopologyMetricFilter;
import argo.utils.Utils;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import parsers.AggregationProfileParser;

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
public class BatchFlipFlopTrends {

    static Logger LOG = LoggerFactory.getLogger(BatchFlipFlopTrends.class);

      private static HashMap<String, ArrayList<String>> metricProfileData;
    private static HashMap<String, String> groupEndpointData;
    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "flipflopsuri", "baseUri", "metricProfileUUID", "key")) {
            System.exit(0);
        }

        env.setParallelism(1);

        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }

        AggregationProfileParser.loadAggrProfileInfo(params.getRequired("baseUri"), params.getRequired("key"), params.get("proxy"));
        metricProfileData = Utils.readMetricDataJson(params.getRequired("baseUri"), params.getRequired("metricProfileUUID"), params.getRequired("key"),params.get("proxy")); //contains the information of the (service, metrics) matches
        groupEndpointData = Utils.readGroupEndpointJson(params.getRequired("baseUri"), params.getRequired("key"),params.get("proxy")); //contains the information of the (service, metrics) matches
        yesterdayData = readInputData(env, params.getRequired("yesterdayData"));
        todayData = readInputData(env, params.getRequired("todayData"));

        DataSet<Tuple5<String, String, String, String, Integer>> criticalData = calcFlipFlops();

        writeToMongo(params.getRequired("flipflopsuri"), criticalData);
// execute program
        env.execute("Flink Batch Java API Skeleton");
    }

    
    // filter yesterdaydata and exclude the ones not contained in topology and metric profile data and get the last timestamp data for each service endpoint metric
    // filter todaydata and exclude the ones not contained in topology and metric profile data , union yesterday data and calculate status changes for each service endpoint metric
    // rank results
   private static DataSet<Tuple5<String, String, String, String, Integer>> calcFlipFlops() {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(metricProfileData, groupEndpointData)).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());

        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(metricProfileData, groupEndpointData));
        DataSet<Tuple5<String, String, String, String, Integer>> reducedData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcServiceEnpointMetricFlipFlop(groupEndpointData));
        if (rankNum != null) {
            reducedData = reducedData.sortPartition(4, Order.DESCENDING).first(rankNum);
        } else {
            reducedData = reducedData.sortPartition(4, Order.DESCENDING);

        }
        return reducedData;
    }
    //read input from file

    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(path);

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class);
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }

    //convert the result in bson format
    public static DataSet<Tuple2<Text, BSONWritable>> convertResultToBSON(DataSet<Tuple5<String, String, String, String, Integer>> in) {

        return in.map(new MapFunction<Tuple5<String, String, String, String, Integer>, Tuple2<Text, BSONWritable>>() {
            int i = 0;

            @Override
            public Tuple2<Text, BSONWritable> map(Tuple5<String, String, String, String, Integer> in) throws Exception {
                BasicDBObject dbObject = new BasicDBObject();
                dbObject.put("group", in.f0.toString());
                dbObject.put("service", in.f1.toString());
                dbObject.put("hostname", in.f2.toString());
                dbObject.put("metric", in.f3.toString());
                dbObject.put("trend", in.f4.toString());

                BSONWritable bson = new BSONWritable(dbObject);
                i++;
                return new Tuple2<Text, BSONWritable>(new Text(String.valueOf(i)), bson);
                /* TODO */
            }
        });
    }

    //write to mongo db
    public static void writeToMongo(String uri, DataSet<Tuple5<String, String, String, String, Integer>> data) {
        DataSet<Tuple2<Text, BSONWritable>> result = convertResultToBSON(data);
        JobConf conf = new JobConf();
        conf.set("mongo.output.uri", uri);

        MongoOutputFormat<Text, BSONWritable> mongoOutputFormat = new MongoOutputFormat<Text, BSONWritable>();
        result.output(new HadoopOutputFormat<Text, BSONWritable>(mongoOutputFormat, conf));
    }

    //convert the result in bson format
}
