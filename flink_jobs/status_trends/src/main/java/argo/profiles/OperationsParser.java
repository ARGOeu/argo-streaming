/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.profiles;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.Serializable;
import org.json.simple.JSONObject;

public class OperationsParser implements Serializable {

    private static final Logger LOG = Logger.getLogger(OperationsParser.class.getName());

    private HashMap<String, Integer> states;
    private HashMap<String, Integer> ops;
    private ArrayList<String> revStates;
    private ArrayList<String> revOps;

    private int[][][] truthTable;

    private String defaultDownState;
    private String defaultMissingState;
    private String defaultUnknownState;

    private boolean order;
    private final String url = "/operations_profiles";

    public OperationsParser() {
        this.states = new HashMap<String, Integer>();
        this.ops = new HashMap<String, Integer>();
        this.revStates = new ArrayList<String>();
        this.revOps = new ArrayList<String>();

        this.truthTable = null;

        this.order = false;

    }

    public OperationsParser(JSONObject jsonObj, boolean _order) {
        this.states = new HashMap<String, Integer>();
        this.ops = new HashMap<String, Integer>();
        this.revStates = new ArrayList<String>();
        this.revOps = new ArrayList<String>();
        this.order = _order;

        this.truthTable = null;
        JSONObject jsonObject = jsonObj;
        //       readApiRequestResult();
    }

    public OperationsParser(boolean _order) {
        this.states = new HashMap<String, Integer>();
        this.ops = new HashMap<String, Integer>();
        this.revStates = new ArrayList<String>();
        this.revOps = new ArrayList<String>();
        this.order = _order;

        this.truthTable = null;
    }

    public String getDefaultDown() {
        return this.defaultDownState;
    }

    public String getDefaultUnknown() {
        return this.defaultUnknownState;
    }

    public int getDefaultUnknownInt() {
        return this.getIntStatus(this.defaultUnknownState);
    }

    public int getDefaultDownInt() {
        return this.getIntStatus(this.defaultDownState);
    }

    public String getDefaultMissing() {
        return this.defaultMissingState;
    }
    public int getDefaultMissingInt() {
        return this.getIntStatus(this.defaultMissingState);
    }

    public void clear() {
        this.states = new HashMap<String, Integer>();
        this.ops = new HashMap<String, Integer>();
        this.revStates = new ArrayList<String>();
        this.revOps = new ArrayList<String>();

        this.truthTable = null;
    }

    public int opInt(int op, int a, int b) {
        int result = -1;
        try {
            result = this.truthTable[op][a][b];
        } catch (IndexOutOfBoundsException ex) {
            LOG.info(ex);
            result = -1;
        }

        return result;
    }

    public int opInt(String op, String a, String b) {

        int opInt = this.ops.get(op);
        int aInt = this.states.get(a);
        int bInt = this.states.get(b);

        return this.truthTable[opInt][aInt][bInt];
    }

    public String op(int op, int a, int b) {
        return this.revStates.get(this.truthTable[op][a][b]);
    }

    public String op(String op, String a, String b) {
        int opInt = this.ops.get(op);
        int aInt = this.states.get(a);
        int bInt = this.states.get(b);

        return this.revStates.get(this.truthTable[opInt][aInt][bInt]);
    }

    public String getStrStatus(int status) {
        return this.revStates.get(status);
    }

    public int getIntStatus(String status) {
        return this.states.get(status);
    }

    public String getStrOperation(int op) {
        return this.revOps.get(op);
    }

    public int getIntOperation(String op) {
        return this.ops.get(op);
    }

    public ArrayList<String> availableStates() {

        return this.revStates;
    }

    public ArrayList<String> availableOps() {
        return this.revOps;
    }

    public void loadJson(File jsonFile) throws IOException {
        // Clear data
        this.clear();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(jsonFile));

            JsonParser json_parser = new JsonParser();
            JsonElement j_element = json_parser.parse(br);
            readJson(j_element);
        } catch (FileNotFoundException ex) {
            LOG.error("Could not open file:" + jsonFile.getName());
            throw ex;

        } catch (JsonParseException ex) {
            LOG.error("File is not valid json:" + jsonFile.getName());
            throw ex;
        } finally {
            // Close quietly without exceptions the buffered reader
            IOUtils.closeQuietly(br);
        }

    }

    public void readJson(JsonElement j_element) {
        JsonObject j_obj = j_element.getAsJsonObject();
        JsonArray j_states = j_obj.getAsJsonArray("available_states");
        JsonArray j_ops = j_obj.getAsJsonArray("operations");
        this.defaultMissingState = j_obj.getAsJsonObject("defaults").getAsJsonPrimitive("missing").getAsString();
        this.defaultDownState = j_obj.getAsJsonObject("defaults").getAsJsonPrimitive("down").getAsString();
        this.defaultUnknownState = j_obj.getAsJsonObject("defaults").getAsJsonPrimitive("unknown").getAsString();
        // Collect the available states
        for (int i = 0; i < j_states.size(); i++) {
            this.states.put(j_states.get(i).getAsString(), i);
            this.revStates.add(j_states.get(i).getAsString());

        }

        // Collect the available operations
        int i = 0;
        for (JsonElement item : j_ops) {
            JsonObject jObjItem = item.getAsJsonObject();
            this.ops.put(jObjItem.getAsJsonPrimitive("name").getAsString(), i);
            this.revOps.add(jObjItem.getAsJsonPrimitive("name").getAsString());
            i++;
        }
        // Initialize the truthtable
        int num_ops = this.revOps.size();
        int num_states = this.revStates.size();
        this.truthTable = new int[num_ops][num_states][num_states];

        for (int[][] surface : this.truthTable) {
            for (int[] line : surface) {
                Arrays.fill(line, -1);
            }
        }

        // Fill the truth table
        for (JsonElement item : j_ops) {
            JsonObject jObjItem = item.getAsJsonObject();
            String opname = jObjItem.getAsJsonPrimitive("name").getAsString();
            JsonArray tops = jObjItem.getAsJsonArray("truth_table");
            // System.out.println(tops);

            for (int j = 0; j < tops.size(); j++) {
                // System.out.println(opname);
                JsonObject row = tops.get(j).getAsJsonObject();

                int a_val = this.states.get(row.getAsJsonPrimitive("a").getAsString());
                int b_val = this.states.get(row.getAsJsonPrimitive("b").getAsString());
                int x_val = this.states.get(row.getAsJsonPrimitive("x").getAsString());
                int op_val = this.ops.get(opname);

                // Fill in truth table
                // Check if order sensitivity is off so to insert two truth
                // values
                // ...[a][b] and [b][a]
                this.truthTable[op_val][a_val][b_val] = x_val;
                if (!this.order) {
                    this.truthTable[op_val][b_val][a_val] = x_val;
                }
            }
        }

    }

    public void loadJsonString(List<String> opsJson) throws JsonParseException {
        // Clear data
        this.clear();

        JsonParser json_parser = new JsonParser();
        // Grab the first - and only line of json from ops data
        JsonElement j_element = json_parser.parse(opsJson.get(0));
        readJson(j_element);
    }

    public int[][][] getTruthTable() {
        return truthTable;
    }

    public void setTruthTable(int[][][] truthTable) {
        this.truthTable = truthTable;
    }

    public HashMap<String, Integer> getStates() {
        return states;
    }

    public void setStates(HashMap<String, Integer> states) {
        this.states = states;
    }

    public HashMap<String, Integer> getOps() {
        return ops;
    }

    public void setOps(HashMap<String, Integer> ops) {
        this.ops = ops;
    }

    public ArrayList<String> getRevStates() {
        return revStates;
    }

    public void setRevStates(ArrayList<String> revStates) {
        this.revStates = revStates;
    }

    public ArrayList<String> getRevOps() {
        return revOps;
    }

    public void setRevOps(ArrayList<String> revOps) {
        this.revOps = revOps;
    }

    public String getDefaultDownState() {
        return defaultDownState;
    }

    public void setDefaultDownState(String defaultDownState) {
        this.defaultDownState = defaultDownState;
    }

    public String getDefaultMissingState() {
        return defaultMissingState;
    }

    public void setDefaultMissingState(String defaultMissingState) {
        this.defaultMissingState = defaultMissingState;
    }

    public String getDefaultUnknownState() {
        return defaultUnknownState;
    }

    public void setDefaultUnknownState(String defaultUnknownState) {
        this.defaultUnknownState = defaultUnknownState;
    }

    public boolean isOrder() {
        return order;
    }

    public void setOrder(boolean order) {
        this.order = order;
    }

    public static Logger getLOG() {
        return LOG;
    }

    public String getUrl() {
        return url;
    }

}