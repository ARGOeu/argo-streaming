package argo.commons.profiles;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

//import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import argo.commons.requests.RequestManager;


/**
 *
 * @author cthermolia
 *
 * OperationsParser, collects data as described in the json received from web
 * api operations profiles request
 */
public class OperationsParser implements Serializable {

    private String id;
    private String name;
    private ArrayList<String> states=new ArrayList<>();
    private DefaultStatus defaults;
    private HashMap<String, HashMap<String, String>> opTruthTable=new HashMap<>();
    private final String url = "/operations_profiles";
  private JSONObject jsonObject ;
    public OperationsParser() {
    }

    public class DefaultStatus implements Serializable {

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

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + Objects.hashCode(this.down);
            hash = 97 * hash + Objects.hashCode(this.missing);
            hash = 97 * hash + Objects.hashCode(this.unknown);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final DefaultStatus other = (DefaultStatus) obj;
            if (!Objects.equals(this.down, other.down)) {
                return false;
            }
            if (!Objects.equals(this.missing, other.missing)) {
                return false;
            }
            if (!Objects.equals(this.unknown, other.unknown)) {
                return false;
            }
            return true;
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

    public OperationsParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }

    public void loadOperationProfile(String uri, String key, String proxy) throws IOException, org.json.simple.parser.ParseException {
         jsonObject = RequestManager.request(uri, key, proxy);
         readApiRequestResult();
    }
    
       public void readApiRequestResult(){
        // A JSON object. Key value pairs are unordered. JSONObject supports java.util.Map interface.
        JSONArray dataList = (JSONArray) jsonObject.get("data");

        Iterator<JSONObject> iterator = dataList.iterator();
      
        while (iterator.hasNext()) {
            JSONObject dataObject = (JSONObject) iterator.next();
            id = (String) dataObject.get("id");
            name = (String) dataObject.get("name");

            JSONArray stateList = (JSONArray) dataObject.get("available_states");
            Iterator<String> stateIter = stateList.iterator();
            while (stateIter.hasNext()) {
                String state = stateIter.next();
                states.add(state);
            }

            JSONObject defaultObject = (JSONObject) dataObject.get("defaults");
            String down = (String) defaultObject.get("down");
            String missing = (String) defaultObject.get("missing");
            String unknown = (String) defaultObject.get("unknown");
            defaults = new DefaultStatus(down, missing, unknown);

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

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode(this.id);
        hash = 97 * hash + Objects.hashCode(this.name);
        hash = 97 * hash + Objects.hashCode(this.states);
        hash = 97 * hash + Objects.hashCode(this.defaults);
        hash = 97 * hash + Objects.hashCode(this.opTruthTable);
        hash = 97 * hash + Objects.hashCode(this.url);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final OperationsParser other = (OperationsParser) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        if (!Objects.equals(this.states, other.states)) {
            return false;
        }
        if (!Objects.equals(this.defaults, other.defaults)) {
            return false;
        }
        if (!Objects.equals(this.opTruthTable, other.opTruthTable)) {
            return false;
        }
        return true;
    }

}
