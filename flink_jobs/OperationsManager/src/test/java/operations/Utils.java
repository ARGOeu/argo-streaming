/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package operations;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * A utils class to process resource files for tests and provide the information
 */
public class Utils {

    public int[][][] readTruthTable() throws IOException, FileNotFoundException, ParseException, java.text.ParseException {

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(Utils.class.getResource("/operations/truthtable.json").getFile()));

            JsonParser json_parser = new JsonParser();
            JsonElement j_element = json_parser.parse(br);
            JsonObject jRoot = j_element.getAsJsonObject();
            JsonArray jData = jRoot.get("data").getAsJsonArray();
            JsonElement jItem = jData.get(0);
            int[][][] truthTable = readJson(jItem);
            return truthTable;
        } catch (FileNotFoundException ex) {

            throw ex;

        }

    }

    private int[][][] readJson(JsonElement j_element) {
        HashMap<String, Integer> operations = new HashMap();
        ArrayList<String> revOps = new ArrayList();
        HashMap<String, Integer> states = new HashMap();
        ArrayList<String> revStates = new ArrayList();

        JsonObject j_obj = j_element.getAsJsonObject();
        JsonArray j_ops = j_obj.getAsJsonArray("operations");
        int i = 0;
        for (JsonElement item : j_ops) {
            String jObjItem = item.getAsString();
            operations.put(jObjItem, i);
            revOps.add(jObjItem);
            i++;
        }
        JsonArray j_states = j_obj.getAsJsonArray("available_states");
        i = 0;
        for (JsonElement item : j_states) {
            String jObjItem = item.getAsString();
            states.put(jObjItem, i);
            revStates.add(jObjItem);
            i++;
        }

        int num_ops = revOps.size();
        int num_states = revStates.size();
        int[][][] truthTable = new int[num_ops][num_states][num_states];

        for (int[][] surface : truthTable) {
            for (int[] line : surface) {
                Arrays.fill(line, -1);
            }
        }
        JsonArray input = j_obj.getAsJsonArray("inputs");

        // Fill the truth table
        for (JsonElement item : input) {
            JsonObject jObjItem = item.getAsJsonObject();
            String opname = jObjItem.getAsJsonPrimitive("name").getAsString();
            JsonArray tops = jObjItem.getAsJsonArray("truth_table");
            // System.out.println(tops);

            for (int j = 0; j < tops.size(); j++) {
                // System.out.println(opname);
                JsonObject row = tops.get(j).getAsJsonObject();

                int a_val = revStates.indexOf(row.getAsJsonPrimitive("a").getAsString());
                int b_val = revStates.indexOf(row.getAsJsonPrimitive("b").getAsString());
                int x_val = revStates.indexOf(row.getAsJsonPrimitive("x").getAsString());
                int op_val = revOps.indexOf(opname);

                // Fill in truth table
                // Check if order sensitivity is off so to insert two truth
                // values
                // ...[a][b] and [b][a]
                truthTable[op_val][a_val][b_val] = x_val;
                truthTable[op_val][b_val][a_val] = x_val;

            }
        }

        int[][][] outputTruthTable = new int[num_ops][num_states][num_states];
        JsonArray output = j_obj.getAsJsonArray("output");

        // Fill the truth table
        for (JsonElement item : output) {
            JsonObject jObjItem = item.getAsJsonObject();
            int op = jObjItem.getAsJsonPrimitive("op").getAsInt();
            int a = jObjItem.getAsJsonPrimitive("a").getAsInt();
            int b = jObjItem.getAsJsonPrimitive("b").getAsInt();
            int x = jObjItem.getAsJsonPrimitive("x").getAsInt();
            outputTruthTable[op][a][b] = x;

        }
        return outputTruthTable;
    }
}
