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
import argo.functions.TopologyMetricFilter;
import argo.functions.TimelineStatusCounter;
import argo.functions.LastTimeStampGroupReduce;
import argo.functions.StatusFilter;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

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

   // public static String metricDataPath;
    //public static String groupEndpointsPath;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        env.setParallelism(1);
      //  metricDataPath = params.get("metricDataPath");
        //groupEndpointsPath = params.get("groupEndpointsPath");
        initializeConfigurationParameters(params,env);
        Integer rankNum = null;
        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        DataSet<MetricData> yesterdayData = readInputData(env, params, "yesterdayData");
        DataSet<MetricData> todayData = readInputData(env, params, "todayData");

        DataSet<Tuple6<String, String, String, String, String, Integer>> rankedData = rankByStatus(env, todayData, yesterdayData);
        filterByStatusAndWrite(rankedData, "critical", rankNum, params.getRequired("criticalResults"));
        filterByStatusAndWrite(rankedData, "warning", rankNum, params.getRequired("warningResults"));
        filterByStatusAndWrite(rankedData, "unknown", rankNum, params.getRequired("unknownResults"));

// execute program
        env.execute("Flink Batch Java API Skeleton");
    }

    //filters the yesterdayData and exclude the ones not in topology and metric profile data and keeps the last timestamp for each service endpoint metric
    //filters the todayData and exclude the ones not in topology and metric profile data, union with yesterdayData and calculates the times each status (CRITICAL,WARNING.UNKNOW) appears
    private static DataSet<Tuple6<String, String, String, String, String, Integer>> rankByStatus(ExecutionEnvironment env, DataSet<MetricData> todayData, DataSet<MetricData> yesterdayData) {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter()).groupBy("hostname", "service", "metric").reduceGroup(new LastTimeStampGroupReduce());

        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter());
        DataSet<Tuple6<String, String, String, String, String, Integer>> rankedData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new TimelineStatusCounter());
        return rankedData;
    }

    // filter the data based on status (CRITICAL,WARNING,UNKNOWN), rank and write top N in seperate files for each status
    private static void filterByStatusAndWrite(DataSet<Tuple6<String, String, String, String, String, Integer>> data, String status, Integer rankNum, String path) {
        DataSet<Tuple6<String, String, String, String, String, Integer>> filteredData = data.filter(new StatusFilter(status));

        if (rankNum != null) {
            filteredData = filteredData.sortPartition(5, Order.DESCENDING).first(rankNum);
        } else {
            filteredData = filteredData.sortPartition(5, Order.DESCENDING);

        }
        filteredData.writeAsText(path, FileSystem.WriteMode.OVERWRITE);
    }

    // reads input from file
    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, ParameterTool params, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(params.getRequired(path));

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class);
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }

    private static void initializeConfigurationParameters(ParameterTool params,ExecutionEnvironment env) {

        Configuration conf = new Configuration();
        conf.setString("groupEndpointsPath", params.get("groupEndpointsPath"));
        conf.setString("metricDataPath", params.get("metricDataPath"));
        env.getConfig().setGlobalJobParameters(conf);

    }
}
