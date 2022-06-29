package operations;

//import argo.utils.RequestManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.Serializable;
import org.apache.commons.io.IOUtils;

/**
 * OperationsManager class implements objects that store the information parsed
 * from a json object containing operation profile data or loaded from a json
 * file *
 *
 * The OperationsManager keeps info of the defined statuses, the defined
 * operations, creates a truth table containing all the combinations of statuses
 * per operation , also it convert string operations and statuses to integer
 * based on their position in the list storage
 */
public class OperationsManager implements Serializable {

    private static final Logger LOG = Logger.getLogger(OperationsManager.class.getName());

    private HashMap<String, Integer> states;
    private HashMap<String, Integer> ops;
    private ArrayList<String> revStates;
    private ArrayList<String> revOps;

    private int[][][] truthTable;

    private String defaultDownState;
    private String defaultMissingState;
    private String defaultUnknownState;

    private boolean order;
    //  private final String url = "/operations_profiles";

    /**
     * Constructs an OperationsManager object initializing fields 
     */
    public OperationsManager() {
        this.states = new HashMap<String, Integer>();
        this.ops = new HashMap<String, Integer>();
        this.revStates = new ArrayList<String>();
        this.revOps = new ArrayList<String>();

        this.truthTable = null;

        this.order = false;
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
    
/**
 * Clears the OperationsManager fields
 */
 
    public void clear() {
        this.states = new HashMap<String, Integer>();
        this.ops = new HashMap<String, Integer>();
        this.revStates = new ArrayList<String>();
        this.revOps = new ArrayList<String>();

        this.truthTable = null;
    }

/**
* Retrieves the status which is a combination of 2 statuses based on the truth table of the operation, as an int
* @param op , the operation (e.g 0, 1)
* @param a , the 1st status (e.g 3 )
* @param b , the 2nd status (e.g 2) 
* @return the final status which is the combination of the two statuses retrieved from the operation's truth table
*/
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
/**
 * Retrieves the status which is a combination of 2 statuses based on the truth table of the operation, as an int
 * @param op , the operation in the form of a string (e.g AND , OR)
 * @param a ,  the 1st status in the form of a string (e.g OK , MISSING)
 * @param b , the 2nd status in the form of a string (e.g OK, MISSING)
 * @return  . the final status which is the combination of the two statuses retrieved from the operation's truth table
 */
    public int opInt(String op, String a, String b) {

        int opInt = this.ops.get(op);
        int aInt = this.states.get(a);
        int bInt = this.states.get(b);

        return this.truthTable[opInt][aInt][bInt];
    }
/**
 * Retrieves the status which is a combination of 2 statuses based on the truth table of the operation, as a string
 * @param op , the operation as an int (e.g 0, 1)
 * @param a , the 1st status as an int (e.g 1, 3)
 * @param b , the 2nd status as an int (e.g 1, 3)
 * @return  the final status  which is the combination of the two statuses , as a string,
 *          retrieved from the operation's truth table
 */
    public String op(int op, int a, int b) {
        return this.revStates.get(this.truthTable[op][a][b]);
    }
/**
 * Retrieves the status which is a combination of 2 statuses based on the truth table of the operation, as a string
 * @param op, the operation as a string (e.g AND, OR)
 * @param a , the 1st status as a string  (e.g OK, MISSING)
 * @param b, the 1st status as a string  (e.g OK , MISSING)
 * @return the final status  which is the combination of the two statuses , as a string,
 *          retrieved from the operation's truth table
 */
    public String op(String op, String a, String b) {
        int opInt = this.ops.get(op);
        int aInt = this.states.get(a);
        int bInt = this.states.get(b);

        return this.revStates.get(this.truthTable[opInt][aInt][bInt]);
    }
/**
* Maps a status as string to an int based on the position of the status in the stored list of statuses
 * @param status a status as an int (e.g 1 ,2)
 * @return  the status as a string 
 */
    public String getStrStatus(int status) {
        return this.revStates.get(status);
    }
/**
  * Maps a status as string to an int 
 * @param status ,a status as a string  (e.g OK,MISSING)
 * @return  the status as an int
 */
    public int getIntStatus(String status) {
        return this.states.get(status);
    }
/**
 * Maps an operation as int to a string based on the position of the operation in the stored list of operations
 * @param op , an operation as an int
 * @return  the operation as a string 
 */
    public String getStrOperation(int op) {
        return this.revOps.get(op);
    }
/**
 * Maps an operation as string to an int 
 * @param op, an operation as a string
 * @return  the operation as an int
 */
    public int getIntOperation(String op) {
        return this.ops.get(op);
    }

    public ArrayList<String> availableStates() {

        return this.revStates;
    }

    public ArrayList<String> availableOps() {
        return this.revOps;
    }
/**
 * reads from a json file and stores the necessary information to the OperationsManager  object fields
 * @param jsonFile a json file containing information about the operation profiles
 * @throws IOException 
 */
    public void loadJson(File jsonFile) throws IOException {
        // Clear data
        this.clear();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(jsonFile));

            JsonParser json_parser = new JsonParser();
            JsonElement j_element = json_parser.parse(br);
            JsonObject jRoot = j_element.getAsJsonObject();
            JsonArray jData = jRoot.get("data").getAsJsonArray();
            JsonElement jItem = jData.get(0);

            readJson(jItem);
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
/**
 * reads from a JsonElement and stores the necessary information to the OperationsManager object fields
 * @param j_element  , a JsonElement containing the operations profiles data
 */
    private void readJson(JsonElement j_element) {
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
/**
 * Calls a JsonParser to read from a list of strings containing the operations profiles data , extracts the JsonElement and 
 * calls the readJson() to read and store the operations profiles data
 * @param opsJson , a list of strings
 * @throws JsonParseException 
 */
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

}
