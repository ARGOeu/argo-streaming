/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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

import argo.functions.timeline.CalcMetricTimelineStatus;
import argo.functions.servicetrends.CalcServiceEndpointTimeline;
import argo.functions.servicetrends.CalcServiceFlipFlop;
import argo.functions.servicetrends.FilterGroupTopology;
import argo.functions.timeline.CalcLastTimeStatus;
import argo.functions.timeline.ProfileServiceFilter;
import argo.functions.timeline.TopologyMetricFilter;
import argo.pojos.MetricTimelinePojo;
import argo.pojos.ServiceEndpTimelinePojo;
import argo.pojos.ServiceFlipFlopPojo;
import argo.utils.Utils;
import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.mapred.MongoOutputFormat;
import java.io.IOException;
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
import org.json.simple.parser.ParseException;
import parsers.AggregationProfileParser;
import parsers.AggregationProfileParser.GroupOps;
import parsers.MetricProfileParser;
import parsers.OperationsParser;
import parsers.ReportParser;
import parsers.TopologyEndpointParser;
import parsers.TopologyGroupParser;
import parsers.TopologyGroupParser.TopologyGroup;

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
public class BatchServiceFlipFlopTrends {

    private static HashMap<String, HashMap<String, String>> opTruthTableMap = new HashMap<>(); // the truth table for the operations to be applied on timeline
    private static HashMap<String, ArrayList<String>> metricProfileData;
    private static HashMap<String, String> topologyGroupEndpointData;
    private static ArrayList<String> topologyGroupData;

    private static DataSet<MetricData> yesterdayData;
    private static DataSet<MetricData> todayData;
    private static Integer rankNum;

    private static TopologyGroupParser topolGroupParser;
    private static TopologyEndpointParser topolEndpointParser;
    private static ReportParser reportParser;
    private static AggregationProfileParser aggregationProfileParser;
    private static MetricProfileParser metricProfileParser;
    private static OperationsParser operationParser;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        //check if all required parameters exist and if not exit program
        if (!Utils.checkParameters(params, "yesterdayData", "todayData", "serviceflipflopsuri", "baseUri", "key")) {
            System.exit(0);
        }

        env.setParallelism(1);

        initializeInputData(params, env);

// calculate on data 
        calcFlipFlops(params);

// execute program
        env.execute("Flink Batch Java API Skeleton");

    }

    private static void initializeInputData(ParameterTool params, ExecutionEnvironment env) throws IOException, ParseException {

        if (params.get("N") != null) {
            rankNum = params.getInt("N");
        }

        reportParser = new ReportParser();
        reportParser.loadReportInfo(params.getRequired("baseUri"), params.getRequired("key"), params.get("proxy"), params.getRequired("reportId"));
        ReportParser.Topology topology = reportParser.getTenantReport().getGroup();

        topolGroupParser = new TopologyGroupParser(params.getRequired("baseUri"), params.getRequired("key"), params.get("proxy"), params.getRequired("date"));
        //ArrayList<TopologyGroupParser.TopologyGroup> groupsList = topolGroupParser.getTopologyGroups().get(topology);

        topolEndpointParser = new TopologyEndpointParser(params.getRequired("baseUri"), params.getRequired("key"), params.get("proxy"), params.getRequired("date"));

        String aggregationId = reportParser.getProfileId(ReportParser.ProfileType.AGGREGATION.name());
        String metricId = reportParser.getProfileId(ReportParser.ProfileType.METRIC.name());
        String operationsId = reportParser.getProfileId(ReportParser.ProfileType.OPERATIONS.name());

        aggregationProfileParser = new AggregationProfileParser(params.getRequired("baseUri"), params.getRequired("key"), params.get("proxy"), aggregationId, params.get("date"));

        metricProfileParser = new MetricProfileParser(params.getRequired("baseUri"), params.getRequired("key"), params.get("proxy"), metricId, params.get("date"));
        metricProfileData = metricProfileParser.getMetricData();

        operationParser = new OperationsParser(params.getRequired("baseUri"), params.getRequired("key"), params.get("proxy"), operationsId, params.get("date"));

        topologyGroupEndpointData = topolEndpointParser.getTopology(aggregationProfileParser.getEndpointGroup().toUpperCase());

        ArrayList<TopologyGroup> topoloGroupList = topolGroupParser.getTopologyGroups().get(topology.getType());
        topologyGroupData = new ArrayList<>();
        for (TopologyGroup group : topoloGroupList) {
            topologyGroupData.add(group.getSubgroup());
        }
        yesterdayData = readInputData(env, params, "yesterdayData");
        todayData = readInputData(env, params, "todayData");

    }

// filter yesterdaydata and exclude the ones not contained in topology and metric profile data and get the last timestamp data for each service endpoint metric
// filter todaydata and exclude the ones not contained in topology and metric profile data , union yesterday data and calculate status changes for each service endpoint metric
// rank results
    private static void calcFlipFlops(ParameterTool params) {

        String hostOperation = aggregationProfileParser.getMetricOp();

        HashMap<String, String> hostnameTruthTable = operationParser.getOpTruthTable().get(hostOperation);

        DataSet<MetricData> filteredYesterdayData = yesterdayData.filter(new TopologyMetricFilter(metricProfileData, topologyGroupEndpointData)).groupBy("hostname", "service", "metric").reduceGroup(new CalcLastTimeStatus());
        DataSet<MetricData> filteredTodayData = todayData.filter(new TopologyMetricFilter(metricProfileData, topologyGroupEndpointData));

        //group data by service enpoint metric and return for each group , the necessary info and a treemap containing timestamps and status
        DataSet<MetricTimelinePojo> serviceEndpointMetricGroupData = filteredTodayData.union(filteredYesterdayData).groupBy("hostname", "service", "metric").reduceGroup(new CalcMetricTimelineStatus(topologyGroupEndpointData));

        //group data by service endpoint  and count flip flops
        //DataSet<ServEndpFlipFlopPojo> serviceEndpointGroupData = serviceEndpointMetricGroupData.groupBy("group", "endpoint", "service").reduceGroup(new CalcServiceEndpointFlipFlop(truthTable));
        DataSet<ServiceEndpTimelinePojo> serviceEndpointGroupData = serviceEndpointMetricGroupData.groupBy("group", "service", "endpoint").reduceGroup(new CalcServiceEndpointTimeline(hostnameTruthTable));

        DataSet<ServiceFlipFlopPojo> serviceGroupData = null;
        for (GroupOps gop : aggregationProfileParser.getGroups()) {

            ArrayList<String> serviceList = new ArrayList<>(gop.getServices().keySet());
            DataSet<ServiceFlipFlopPojo> data = serviceEndpointGroupData.filter(new FilterGroupTopology(topologyGroupData)).filter(new ProfileServiceFilter(serviceList, gop.getName())).groupBy("group", "service").reduceGroup(new CalcServiceFlipFlop(gop.getServices(), operationParser.getOpTruthTable(), gop.getName(), serviceList));
            if (serviceGroupData == null) {
                serviceGroupData = data;
            } else {
                serviceGroupData = serviceGroupData.union(data);
            }
            if (rankNum != null) { //sort and rank data
                serviceGroupData = serviceGroupData.sortPartition("flipflops", Order.DESCENDING).first(rankNum);
            } else {
                serviceGroupData = serviceGroupData.sortPartition("flipflops", Order.DESCENDING);
            }
        }
        writeToMongo(params.getRequired("serviceflipflopsuri"), serviceGroupData);

    }

    private static DataSet<MetricData> readInputData(ExecutionEnvironment env, ParameterTool params, String path) {
        DataSet<MetricData> inputData;
        Path input = new Path(params.getRequired(path));

        AvroInputFormat<MetricData> inputAvroFormat = new AvroInputFormat<MetricData>(input, MetricData.class
        );
        inputData = env.createInput(inputAvroFormat);
        return inputData;
    }

    //convert the result in bson format
    public static DataSet<Tuple2<Text, BSONWritable>> convertResultToBSON(DataSet<ServiceFlipFlopPojo> in) {

        return in.map(new MapFunction<ServiceFlipFlopPojo, Tuple2<Text, BSONWritable>>() {
            int i = 0;

            @Override
            public Tuple2<Text, BSONWritable> map(ServiceFlipFlopPojo in) throws Exception {
                BasicDBObject dbObject = new BasicDBObject();
                dbObject.put("profile", in.getProfileName().toString());
                dbObject.put("group", in.getGroup().toString());
                dbObject.put("service", in.getService().toString());
                dbObject.put("trend", in.getFlipflops());

                BSONWritable bson = new BSONWritable(dbObject);
                i++;
                return new Tuple2<Text, BSONWritable>(new Text(String.valueOf(i)), bson);
                /* TODO */
            }
        });
    }

    //write to mongo db
    public static void writeToMongo(String uri, DataSet<ServiceFlipFlopPojo> data) {
        DataSet<Tuple2<Text, BSONWritable>> result = convertResultToBSON(data);
        JobConf conf = new JobConf();
        conf.set("mongo.output.uri", uri);

        MongoOutputFormat<Text, BSONWritable> mongoOutputFormat = new MongoOutputFormat<Text, BSONWritable>();
        result.output(new HadoopOutputFormat<Text, BSONWritable>(mongoOutputFormat, conf));
    }

}
