/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package flink.jobs.timelines;

import timelines.Utils;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class TimelineUtils {

    public TimelineJson readTimelines() throws IOException, FileNotFoundException, ParseException, java.text.ParseException {

        JSONObject timelineJSONObj = readJsonFromFile(TimelineUtils.class.getResource("/timelines/timeline.json").getFile());
        TimelineJson timelinejson = buildTimelines(timelineJSONObj);
        return timelinejson;
    }

    private JSONObject readJsonFromFile(String path) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();
        URL url = TimelineUtils.class.getResource(path);
        Object obj = parser.parse(new FileReader(path));

        JSONObject jsonObject = (JSONObject) obj;

        return jsonObject;
    }

    public TimelineJson buildTimelines(JSONObject jsonObject) throws java.text.ParseException {

        ArrayList<String> states = new ArrayList<>();
        ArrayList<String> operations = new ArrayList<>();
        ArrayList<TreeMap> inputTimelines = new ArrayList<>();
        TreeMap<DateTime, Integer> outputTimeline = new TreeMap();
        JSONObject dataObject = (JSONObject) jsonObject.get("data");

        JSONArray stateList = (JSONArray) dataObject.get("available_states");
        JSONArray operationList = (JSONArray) dataObject.get("operations");
        String operation = (String) dataObject.get("operation");
        Iterator<String> operationsIter = operationList.iterator();

        while (operationsIter.hasNext()) {
            String op = operationsIter.next();
            operations.add(op);
        }
        JSONArray inputs = (JSONArray) dataObject.get("inputs");
        JSONObject output = (JSONObject) dataObject.get("output");
        Iterator<String> stateIter = stateList.iterator();
        while (stateIter.hasNext()) {
            String state = stateIter.next();
            states.add(state);
        }

        Iterator<JSONObject> inputIter = inputs.iterator();
        while (inputIter.hasNext()) {
            JSONObject timelineJSONObj = inputIter.next();
            JSONArray timestampList = (JSONArray) timelineJSONObj.get("timestamps");
            Iterator<JSONObject> timeIter = timestampList.iterator();
            TreeMap<DateTime, Integer> map = new TreeMap();
            while (timeIter.hasNext()) {
                JSONObject timestatus = (JSONObject) timeIter.next();
                String time = (String) timestatus.get("timestamp");
                String status = (String) timestatus.get("status");
                map.put(Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", time), states.indexOf(status));
                inputTimelines.add(map);
            }

        }

        JSONArray timestampList = (JSONArray) output.get("timestamps");
        Iterator<JSONObject> timeIter = timestampList.iterator();

        while (timeIter.hasNext()) {
            JSONObject timestatus = (JSONObject) timeIter.next();
            String time = (String) timestatus.get("timestamp");
            String status = (String) timestatus.get("status");
            outputTimeline.put(Utils.convertStringtoDate("yyyy-MM-dd'T'HH:mm:ss'Z'", time), states.indexOf(status));

        }

        JSONArray opTruthTable = (JSONArray) dataObject.get("operation_truth_table");

        Iterator<JSONObject> opTruthTableIter = opTruthTable.iterator();
        int[][][] table = new int[operations.size()][states.size()][states.size()];
        for (int[][] surface : table) {
            for (int[] line : surface) {
                Arrays.fill(line, -1);
            }
        }
        while (opTruthTableIter.hasNext()) {
            JSONObject truthOperationObj = (JSONObject) opTruthTableIter.next();
            String truthOp = (String) truthOperationObj.get("name");
            int truthOpInt = operations.indexOf(truthOp);
            JSONArray truthTable = (JSONArray) truthOperationObj.get("truth_table");
            Iterator<JSONObject> truthTableIter = truthTable.iterator();
            while (truthTableIter.hasNext()) {

                JSONObject truthTableObj = (JSONObject) truthTableIter.next();
                String a = (String) truthTableObj.get("a");
                int aInt = states.indexOf(a);
                String b = (String) truthTableObj.get("b");
                int bInt = states.indexOf(b);
                String x = (String) truthTableObj.get("b");
                int xInt = states.indexOf(x);
                table[truthOpInt][aInt][bInt] = xInt;

            }
        }
        TimelineJson timelineJsonObject = new TimelineJson(inputTimelines, outputTimeline, operations.indexOf(operation),table,states);
        return timelineJsonObject;
    }

    public class TimelineJson {

        private ArrayList<TreeMap> inputTimelines;
        private TreeMap outputTimeline;
        private Integer operation;
        private int[][][] truthTable;
        private ArrayList<String> states;

        public TimelineJson(ArrayList<TreeMap> inputTimelines, TreeMap outputTimeline, Integer operation, int[][][] truthTable, ArrayList<String> states) {
            this.inputTimelines = inputTimelines;
            this.outputTimeline = outputTimeline;
            this.operation = operation;
            this.truthTable = truthTable;
            this.states = states;
        }

        public ArrayList<TreeMap> getInputTimelines() {
            return inputTimelines;
        }

        public void setInputTimelines(ArrayList<TreeMap> inputTimelines) {
            this.inputTimelines = inputTimelines;
        }

        public TreeMap getOutputTimeline() {
            return outputTimeline;
        }

        public void setOutputTimeline(TreeMap outputTimeline) {
            this.outputTimeline = outputTimeline;
        }

        public Integer getOperation() {
            return operation;
        }

        public void setOperation(Integer operation) {
            this.operation = operation;
        }

        public int[][][] getTruthTable() {
            return truthTable;
        }

        public void setTruthTable(int[][][] truthTable) {
            this.truthTable = truthTable;
        }

        public ArrayList<String> getStates() {
            return states;
        }

        public void setStates(ArrayList<String> states) {
            this.states = states;
        }
    }
        

}
