/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.profiles;

import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 *
 * OperationsParser, collects data as described in the json received from web
 * api operations profiles request
 */
public class OperationsParser implements Serializable {
	private static final Logger LOG = Logger.getLogger(OperationsParser.class.getName());
    private String id;
    private String name;
    private ArrayList<String> states = new ArrayList<>();
    private DefaultStatus defaults;
    private HashMap<String, HashMap<String, String>> opTruthTable = new HashMap<>();
    private final String url = "/operations_profiles";
    private JSONObject jsonObject;
    private int[][][] truthTable;
    private ArrayList<String> operations=new ArrayList<>();

    public OperationsParser() {
    }

    public OperationsParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }

    private class DefaultStatus implements Serializable {

        private String down;
        private String missing;
        private String unknown;

        public DefaultStatus() {
        }

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

        public void setDown(String down) {
            this.down = down;
        }

        public void setMissing(String missing) {
            this.missing = missing;
        }

        public void setUnknown(String unknown) {
            this.unknown = unknown;
        }

    }

    public OperationsParser(String apiUri, String key, String proxy, String operationsId, String dateStr) throws IOException, ParseException {
        String uri = apiUri + url;
        if (dateStr == null) {
            uri = uri + operationsId;
        } else {
            uri = uri + "?date=" + dateStr;
        }
        loadOperationProfile(uri, key, proxy);
    }

    private void loadOperationProfile(String uri, String key, String proxy) throws IOException, org.json.simple.parser.ParseException {
        jsonObject = RequestManager.request(uri, key, proxy);
        readApiRequestResult();
    }

    public void readApiRequestResult() {

        // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
        JSONArray dataList = (JSONArray) jsonObject.get("data");

        Iterator<JSONObject> iterator = dataList.iterator();

        while (iterator.hasNext()) {
            JSONObject dataObject = (JSONObject) iterator.next();
            this.id = (String) dataObject.get("id");
            this.name = (String) dataObject.get("name");

            JSONArray stateList = (JSONArray) dataObject.get("available_states");
            Iterator<String> stateIter = stateList.iterator();
            while (stateIter.hasNext()) {
                String state = stateIter.next();
                this.states.add(state);
            }

            JSONObject defaultObject = (JSONObject) dataObject.get("defaults");
            String down = (String) defaultObject.get("down");
            String missing = (String) defaultObject.get("missing");
            String unknown = (String) defaultObject.get("unknown");
            this.defaults = new DefaultStatus(down, missing, unknown);

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
                this.opTruthTable.put(opName, truthTable);
                this.operations.add(opName);
            }
        }
        initTruthTable();
    }

    private void initTruthTable() {

        int numOps = this.opTruthTable.keySet().size();
        int numStates = this.states.size();
        this.truthTable = new int[numOps][numStates][numStates];
        for (int[][] surface : this.truthTable) {
            for (int[] line : surface) {
                Arrays.fill(line, -1);
            }
        }
        int opPos = 0;
        for (String op : this.opTruthTable.keySet()) {

            HashMap<String, String> opStates = this.opTruthTable.get(op);
            for (String key : opStates.keySet()) {
                String[] stateArr = splitStates(key);
                int a = this.states.indexOf(stateArr[0]);
                int b = this.states.indexOf(stateArr[1]);
                int x = this.states.indexOf(opStates.get(key));
                this.truthTable[opPos][a][b] = x;
            }
            opPos++;
        }

    }

    private String[] splitStates(String state) {
        String[] arrOfStr = state.split("-");
        return arrOfStr;

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
        
        int opInt = this.operations.indexOf(op);
        int aInt = this.states.indexOf(a);
        int bInt = this.states.indexOf(b);

        return this.truthTable[opInt][aInt][bInt];
    }
    
    public String getStrStatus(int status) {
		return this.states.get(status);
	}

	public int getIntStatus(String status) {
		return this.states.indexOf(status);
	}

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
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

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setStates(ArrayList<String> states) {
        this.states = states;
    }

    public void setDefaults(DefaultStatus defaults) {
        this.defaults = defaults;
    }

    public void setOpTruthTable(HashMap<String, HashMap<String, String>> opTruthTable) {
        this.opTruthTable = opTruthTable;
    }
    
    
    public String getStrOperation(int op) {
		return this.operations.get(op);
	}

	public int getIntOperation(String op) {
		return this.operations.indexOf(op);
	}

    public String getStatusFromTruthTable(String operation, String astatus, String bstatus) {
        String finalStatus = null;
        HashMap<String, String> truthTable = this.opTruthTable.get(operation);
        String status = astatus + "-" + bstatus;
        if (truthTable.containsKey(status)) {
            finalStatus = truthTable.get(status);
        } else { //reverse status combination
            status = bstatus + "-" + astatus;
            finalStatus = truthTable.get(status);
        }
        return finalStatus;
    }

    public int[][][] getTruthTable() {
        return truthTable;
    }

    public void setTruthTable(int[][][] truthTable) {
        this.truthTable = truthTable;
    }

    public ArrayList<String> getOperations() {
        return operations;
    }

    public void setOperations(ArrayList<String> operations) {
        this.operations = operations;
    }
    
    

}
