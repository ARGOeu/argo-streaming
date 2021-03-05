/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parsers;

import argo.utils.RequestManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class OperationsParser {

    private String id;
    private String name;
    private ArrayList<String> states;
    private DefaultStatus defaults;
    private HashMap<String, HashMap<String, String>> opTruthTable;
    
    public class DefaultStatus{
    
        private String down;
        private String missing;
        private String unknown;

        public DefaultStatus(String down, String missing, String unknown) {
            this.down = down;
            this.missing = missing;
            this.unknown = unknown;
        }

        public String getDown() {
            return down;
        }

        public String getMissing() {
            return missing;
        }

        public String getUnknown() {
            return unknown;
        }

        
    }

    public OperationsParser(String baseUri, String key, String proxy, String operationsId,String date) throws IOException, ParseException {
    
        loadOperationProfile(baseUri, key, proxy, operationsId, date);
    }

    private HashMap<String, HashMap<String, String>> loadOperationProfile(String baseUri, String key, String proxy, String operationsId,String date) throws IOException, org.json.simple.parser.ParseException {
        JSONObject jsonObject = RequestManager.getOperationProfileRequest(baseUri, key, proxy, operationsId,date);

        // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
        JSONArray dataList = (JSONArray) jsonObject.get("data");

        Iterator<JSONObject> iterator = dataList.iterator();
         opTruthTable = new HashMap<>();
         states=new ArrayList<>();
        while (iterator.hasNext()) {
            JSONObject dataObject = (JSONObject) iterator.next();
            id = (String) dataObject.get("id");
            name = (String) dataObject.get("name");

            JSONArray stateList = (JSONArray) dataObject.get("available_states");
            Iterator<String> stateIter = stateList.iterator();
            while (stateIter.hasNext()) {
                String state =  stateIter.next();
                states.add(state);
            }
            
            JSONObject defaultObject = (JSONObject) dataObject.get("defaults");
            String down=(String)defaultObject.get("down");
            String missing=(String)defaultObject.get("missing");
            String unknown=(String)defaultObject.get("unknown");
             defaults=new DefaultStatus(down, missing, unknown);
            
           
            JSONArray operationList = (JSONArray) dataObject.get("operations");
            Iterator<JSONObject> opIterator = operationList.iterator();
            while (opIterator.hasNext()) {
                JSONObject operationObject = (JSONObject) opIterator.next();
                String opName = (String) operationObject.get("name");
                JSONArray truthtable = (JSONArray) operationObject.get("truth_table");
                Iterator<JSONObject> truthTableIter = truthtable.iterator();
                HashMap<String, String> truthTable = new HashMap<>();
                while (truthTableIter.hasNext()) {
                    JSONObject truthEntry = (JSONObject) truthTableIter.next();
                    String a = (String) truthEntry.get("a");
                    String b = (String) truthEntry.get("b");
                    String x = (String) truthEntry.get("x");

                    truthTable.put(a + "-" + b, x);
                }
                opTruthTable.put(opName, truthTable);
            }
        }
        return opTruthTable;

    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public ArrayList<String> getStates() {
        return states;
    }

    public DefaultStatus getDefaults() {
        return defaults;
    }

    public HashMap<String, HashMap<String, String>> getOpTruthTable() {
        return opTruthTable;
    }

    
    
}
