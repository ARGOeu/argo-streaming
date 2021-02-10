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

import argo.functions.FlipFlopStatusCounter;
import argo.functions.LastTimeStampGroupReduce;
import argo.functions.TopologyMetricFilter;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class BatchFlipFlopTrends {

    static Logger LOG = LoggerFactory.getLogger(BatchFlipFlopTrends.class);

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        if (params.getRequired("yesterdayData") == null || params.getRequired("todayData") == null || params.getRequired("groupEndpointsPath") == null
                || params.getRequired("metricDataPath") == null || params.getRequired("flipflopResults") == null) {
            LOG.info("Program ended due to not found required parameters");
            System.exit(0);
        }
        env.setParallelism(1);

        Integer rankNum = null;
        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }
        DataSet<MetricData> yesterdayData = readInputData(env, params, "yesterdayData");
        DataSet<MetricData> todayData = readInputData(env, params, "todayData");

        DataSet<Tuple5<String, String, String, String, Integer>> criticalData = calcFlipFlops(params, rankNum, todayData, yesterdayData);
        criticalData.writeAsText(params.getRequired("flipflopResults"), FileSystem.WriteMode.OVERWRITE);

// execute program
        env.execute("Flink Batch Java API Skeleton");
    }

    // filter yesterdaydata and exclude the ones not contained in topology and metric profile data and get the last timestamp data for each service endpoint metric
    // filter todaydata and exclude the ones not contained in topology and metric profile data , union yesterday data and calculate status changes for each service endpoint metric
    // rank results
    private static DataSet<Tuple5<String, String, String, String, Integer>> calcFlipFlops(ParameterTool params, Integer rankNum, DataSet<MetricData> todayData, DataSet<MetricData> yesterdayData) {

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(params)).groupBy("hostname", "service", "metric").reduceGroup(new LastTimeStampGroupReduce());

        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(params));
        DataSet<Tuple5<String, String, String, String, Integer>> reducedData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new FlipFlopStatusCounter(params));
        if (rankNum != null) {
            reducedData = reducedData.sortPartition(4, Order.DESCENDING).first(rankNum);
        } else {
            reducedData = reducedData.sortPartition(4, Order.DESCENDING);

        }
        return reducedData;

    }
    //read input from file

    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, ParameterTool params, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(params.getRequired(path));

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class);
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }


}
