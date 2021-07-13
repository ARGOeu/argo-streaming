package ops;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

/**
 * @author kaggis
 *
 */
public class ThresholdManager {

	private static final Logger LOG = Logger.getLogger(ThresholdManager.class.getName());

	// Nested map that holds rule definitions: "groups/hosts/metrics" -> label ->
	// threshold
	// rules"
	private Map<String, Map<String, Threshold>> rules;

	// Reverse index checks for group, host, metrics
	private HashSet<String> metrics;
	private HashSet<String> hosts;
	private HashSet<String> groups;
	private String aggregationOp = "AND";

	public Map<String, Map<String, Threshold>> getRules() {
		return this.rules;
	}

	/**
	 * Threshold class implements objects that hold threshold values as they are
	 * parsed by a threshold expression such as the following one:
	 * 
	 * label=30s;0:50,50:100,0,100
	 * 
	 * A Threshold object can be directly constructed from a string including an
	 * expression as the above
	 * 
	 * Each threshold object stores the threshold expression and the individual
	 * parsed items such as value, uom, warning range, critical range and min,max
	 * values
	 *
	 */
	class Threshold {

		private static final String defWarning = "WARNING";
		private static final String defCritical = "CRITICAL";

		private String expression;
		private String label;
		private Float value;
		private String uom;
		private Range warning;
		private Range critical;
		private Float min;
		private Float max;
		
	

		/**
		 * Constructs a threshold from a string containing a threshold expression
		 * 
		 * @param expression
		 *            A string containing a threshold exception as the following one:
		 *            label=30s;0:50,50:100,0,100
		 * 
		 */
		public Threshold(String expression) {
			Threshold temp = parseAndSet(expression);
			this.expression = temp.expression;
			this.label = temp.label;
			this.value = temp.value;
			this.uom = temp.uom;
			this.warning = temp.warning;
			this.critical = temp.critical;
			this.min = temp.min;
			this.max = temp.max;

		}

		/**
		 * Create a new threshold object by providing each parameter
		 * 
		 * @param expression
		 *            string containing the threshold expression
		 * @param label
		 *            threshold label
		 * @param value
		 *            threshold value
		 * @param uom
		 *            unit of measurement - optional
		 * @param warning
		 *            a range determining warning statuses
		 * @param critical
		 *            a range determining critical statuses
		 * @param min
		 *            minimum value available for this threshold
		 * @param max
		 *            maximum value available for this threshold
		 */
		public Threshold(String expression, String label, float value, String uom, Range warning, Range critical,
				float min, float max) {

			this.expression = expression;
			this.label = label;
			this.value = value;
			this.uom = uom;
			this.warning = warning;
			this.critical = critical;
			this.min = min;
			this.max = max;
		}

		public String getExpression() {
			return expression;
		}

		public String getLabel() {
			return label;
		}

		public float getValue() {
			return value;
		}

		public String getUom() {
			return uom;
		}

		public Range getWarning() {
			return warning;
		}

		public Range getCritical() {
			return critical;
		}

		public float getMin() {
			return min;
		}

		public float getMax() {
			return max;
		}

		/**
		 * Parses a threshold expression string and returns a Threshold object
		 * 
		 * @param threshold
		 *            string containing the threshold expression
		 * @return Threshold object
		 */
		public Threshold parseAndSet(String threshold) {

			String pThresh = threshold;
			String curLabel = "";
			String curUom = "";
			Float curValue = Float.NaN;
			Range curWarning = new Range(); // empty range
			Range curCritical = new Range(); // emtpy range
			Float curMin = Float.NaN;
			Float curMax = Float.NaN;
			// find label by splitting at =
			String[] tokens = pThresh.split("=");
			// Must have two tokens to continue, label=something
			if (tokens.length == 2) {
				curLabel = tokens[0];

				// Split right value by ; to find the array of arguments
				String[] subtokens = tokens[1].split(";");
				// Must have size > 0 at least a value
				if (subtokens.length > 0) {
					curUom = getUOM(subtokens[0]);
					curValue = Float.parseFloat(subtokens[0].replaceAll(curUom, ""));
					if (subtokens.length > 1) {
						// iterate over rest of subtokens
						for (int i = 1; i < subtokens.length; i++) {
							if (i == 1) {
								// parse warning range
								curWarning = new Range(subtokens[i]);
								continue;
							} else if (i == 2) {
								// parse critical
								curCritical = new Range(subtokens[i]);
								continue;
							} else if (i == 3) {
								// parse min
								curMin = Float.parseFloat(subtokens[i]);
								continue;
							} else if (i == 4) {
								// parse min
								curMax = Float.parseFloat(subtokens[i]);
							}
						}
					}

				}

			}

			return new Threshold(threshold, curLabel, curValue, curUom, curWarning, curCritical, curMin, curMax);

		}

		/**
		 * Reads a threshold string value and extracts the unit of measurement if
		 * present
		 * 
		 * @param value
		 *            String containing a representation of the value and uom
		 * @return String representing the uom.
		 */
		public String getUOM(String value) {
			// check if ends with digit
			if (Character.isDigit(value.charAt(value.length() - 1))) {
				return "";
			}

			// check if ends with seconds
			if (value.endsWith("s"))
				return "s";
			if (value.endsWith("us"))
				return "us";
			if (value.endsWith("ms"))
				return "ms";
			if (value.endsWith("%"))
				return "%";
			if (value.endsWith("B"))
				return "B";
			if (value.endsWith("KB"))
				return "KB";
			if (value.endsWith("MB"))
				return "MB";
			if (value.endsWith("TB"))
				return "TB";
			if (value.endsWith("c"))
				return "c";

			// Not valid range
			throw new RuntimeException("Invalid Unit of measurement: " + value);

		}

		/**
		 * Checks an external value against a threshold's warning,critical ranges. If a
		 * range contains the value (warning or critical) the corresponding status is
		 * returned as string "WARNING" or "CRITICAL". If the threshold doesn't provide
		 * the needed data to decide on status an "" is returned back.
		 * 
		 * @return string with the status result "WARNING", "CRITICAL"
		 */
		public String calcStatusWithValue(Float value) {

			if (!Float.isFinite(this.value))
				return "";
			if (!this.warning.isUndef()) {
				if (this.warning.contains(value))
					return defWarning;
			}
			if (!this.critical.isUndef()) {
				if (this.critical.contains(value))
					return defCritical;
			}

			return "";
		}

		/**
		 * Checks a threshold's value against warning,critical ranges. If a range
		 * contains the value (warning or critical) the corresponding status is returned
		 * as string "WARNING" or "CRITICAL". If the threshold doesn't provide the
		 * needed data to decide on status an "" is returned back.
		 * 
		 * @return string with the status result "WARNING", "CRITICAL"
		 */
		public String calcStatus() {

			if (!Float.isFinite(this.value))
				return "";
			if (!this.warning.isUndef()) {
				if (this.warning.contains(this.value))
					return defWarning;
			}
			if (!this.critical.isUndef()) {
				if (this.critical.contains(this.value))
					return defCritical;
			}

			return "";
		}

		public String toString() {
			String strWarn = "";
			String strCrit = "";
			String strMin = "";
			String strMax = "";

			if (this.warning != null)
				strWarn = this.warning.toString();
			if (this.critical != null)
				strCrit = this.critical.toString();
			if (this.min != null)
				strMin = this.min.toString();
			if (this.max != null)
				strMax = this.max.toString();

			return "[expression=" + this.expression + ", label=" + this.label + ", value=" + this.value + ", uom="
					+ this.uom + ", warning=" + strWarn + ", critical=" + strCrit + ", min=" + strMin + ", max="
					+ strMax + ")";
		}

	}

	/**
	 * Range implements a simple object that holds a threshold's critical or warning
	 * range. It includes a floor,ceil as floats and an exclude flag when a range is
	 * supposed to be used for exclusion and not inclusion. The threshold spec uses
	 * an '@' character in front of a range to define inversion(exclusion)
	 * 
	 * Inclusion assumes that floor < value < ceil and not floor <= value <= ceil
	 *
	 */
	class Range {
		Float floor;
		Float ceil;
		Boolean exclude;

		/**
		 * Creates an empty range. Invert is false and limits are NaN
		 */
		public Range() {
			this.floor = Float.NaN;
			this.ceil = Float.NaN;
			this.exclude = false;
		}

		/**
		 * Creates a range by parameters
		 * 
		 * @param floor
		 *            Float that defines the lower limit of the range
		 * @param ceil
		 *            Float that defines the upper limit of the range
		 * @param exclude
		 *            boolean that defines if the range is used for inclusion (true) or
		 *            exlusion (false)
		 */
		public Range(Float floor, Float ceil, Boolean exclude) {
			this.floor = floor;
			this.ceil = ceil;
			this.exclude = exclude;
		}

		/**
		 * Creates a range by parsing a range expression string like the following one:
		 * '0:10'
		 * 
		 * @param range
		 *            string including a range expression
		 */
		public Range(String range) {
			Range tmp = parseAndSet(range);
			this.floor = tmp.floor;
			this.ceil = tmp.ceil;
			this.exclude = tmp.exclude;
		}

		/**
		 * Checks if a Range is undefined (float,ceiling are NaN)
		 * 
		 * @return boolean
		 */
		public boolean isUndef() {
			return this.floor == Float.NaN || this.ceil == Float.NaN;
		}

		/**
		 * Checks if a value is included in range (or truly excluded if range is an
		 * exclusion)
		 * 
		 * @param value
		 *            Float
		 * @return boolean
		 */
		public boolean contains(Float value) {
			boolean result = value > this.floor && value < this.ceil;
			if (this.exclude) {
				return !result;
			}
			return result;
		}

		/**
		 * Parses a range expression string and creates a Range object Range expressions
		 * can be in the following forms:
		 * <ul>
		 * <li>10 - range starting from 0 to 10</li>
		 * <li>10: - range starting from 10 to infinity</li>
		 * <li>~:20 - range starting from negative inf. up to 20</li>
		 * <li>20:30 - range between two numbers</li>
		 * <li>@20:30 - inverted range, excludes betweeen two numbers
		 * </ul>
		 * 
		 * @param expression
		 *            String containing a range expression
		 * @return
		 */
		public Range parseAndSet(String expression) {
			String parsedRange = expression;
			Float curFloor = 0F;
			Float curCeil = 0F;
			boolean curInv = false;
			if (parsedRange.replaceAll(" ", "").equals("")) {
				return new Range();
			}
			// check if invert
			if (parsedRange.startsWith("@")) {
				curInv = true;
				// after check remove @ from range string
				parsedRange = parsedRange.replaceAll("^@", "");
			}

			// check if range string doesn't have separator :
			if (!parsedRange.contains(":")) {
				// then we are in the case of a single number like 10
				// which defines the rule 0 --> 10 so
				curFloor = 0F;
				curCeil = Float.parseFloat(parsedRange);

				return new Range(curFloor, curCeil, curInv);
			}

			// check if range end with separator :
			if (parsedRange.endsWith(":")) {
				parsedRange = parsedRange.replaceAll(":$", "");
				// then we are in the case of a signle number like 10:
				// which defines the rule 10 --> positive infinity
				curFloor = Float.parseFloat(parsedRange);
				curCeil = Float.POSITIVE_INFINITY;
				return new Range(curFloor, curCeil, curInv);
			}

			// tokenize string without prefixes
			String[] tokens = parsedRange.split(":");
			if (tokens.length == 2) {
				// check if token[0] is negative infinity ~
				if (tokens[0].equalsIgnoreCase("~")) {
					curFloor = Float.NEGATIVE_INFINITY;
				} else {
					curFloor = Float.parseFloat(tokens[0]);
				}

				curCeil = Float.parseFloat(tokens[1]);
				return new Range(curFloor, curCeil, curInv);
			}

			// Not valid range
			throw new RuntimeException("Invalid threshold: " + expression);

		}

		public String toString() {
			return "(floor=" + this.floor + ",ceil=" + this.ceil + ",invert=" + this.exclude.toString() + ")";
		}

	}

	/**
	 * Creates a Manager that parses rules files with thresholds and stores them
	 * internally as objects. A ThresholdManager can be used to automatically
	 * calculate statuses about a monitoring item (group,host,metric) based on the
	 * most relevant threshold rules stored in it.
	 */
	public ThresholdManager() {

		this.rules = new HashMap<String, Map<String, Threshold>>();
		this.hosts = new HashSet<String>();
		this.groups = new HashSet<String>();
		this.metrics = new HashSet<String>();

	}
	
	/**
	 * Return the default operation when aggregating statuses generated from multiple threshold rules
	 * @return
	 */
	public String getAggregationOp() {
		return this.aggregationOp;
	}
	
	
	/**
	 * @param op string with the name of the operation to be used in the aggregation (AND,OR,custom one)
	 */
	public void setAggregationOp(String op) {
		this.aggregationOp = op;
	}

	/**
	 * Returns a status calculation for a specific rule key Each rule key is defined
	 * as follows: 'group/host/metric' and leads to a threshold rule. Group and host
	 * parts are optional as such: 'group//metric' or '/host/metric' or '//metric'
	 * 
	 * @param rule
	 *            string containing a rule key
	 * @param opsMgr
	 *            an OpsManager Object to handle status aggregations
	 * @param opType
	 *            an OpsManager operation to be used (like 'OR', 'AND')
	 * @return string with status result
	 */
	public String getStatusByRule(String rule, OpsManager opsMgr, String opType) {

		if (!rules.containsKey(rule))
			return "";
		String status = "";
		Map<String, Threshold> tholds = rules.get(rule);
		for (Entry<String, Threshold> thold : tholds.entrySet()) {
			// first step
			if (status == "") {
				status = thold.getValue().calcStatus();
				continue;
			}
			String statusNext = thold.getValue().calcStatus();
			if (statusNext != "") {
				status = opsMgr.op(opType, status, statusNext);
			}
		}
		return status;
	}

	/**
	 * Returns a status calculation for a specific rule key Each rule key is defined
	 * as follows: 'group/host/metric' and leads to a threshold rule. Group and host
	 * parts are optional as such: 'group//metric' or '/host/metric' or '//metric'
	 * 
	 * @param rule
	 *            string containing a rule key
	 * @param opsMgr
	 *            an OpsManager Object to handle status aggregations
	 * @param opType
	 *            an OpsManager operation to be used (like 'OR', 'AND')
	 * @return string array with two elements. First element is the status result and second one the rule applied
	 */
	public String[] getStatusByRuleAndValues(String rule, OpsManager opsMgr, String opType, Map<String, Float> values) {
		
		if (!rules.containsKey(rule))
			return new String[] {"",""};
		String status = "";
		String explain = "";
		Map<String, Threshold> tholds = rules.get(rule);
		
		for ( Entry<String,Float> value : values.entrySet()) {
			String label = value.getKey();
			if (tholds.containsKey(label)) {
				Threshold th = tholds.get(label);
				// first step
				if (status == "") {
					
					status = th.calcStatusWithValue(value.getValue());
					explain = th.getExpression();
					continue;
				}
				
				String statusNext = th.calcStatusWithValue(value.getValue());
				
				if (statusNext != "") {
					status = opsMgr.op(opType, status, statusNext);
					explain = explain + " " + th.getExpression();
				}
			}
		}
		
		
		return new String[]{status,explain};

	}

	/**
	 * Gets the most relevant rule based on a monitoring item (group,host,metric)
	 * using the following precedence (specific to least specific) (group, host,
	 * metric) #1 ( , host, metric) #2 (group, , metric) #3 ( , , metric) #4
	 * 
	 * @param group
	 *            string with name of the monitored endpoint group
	 * @param host
	 *            string with name of the monitored host
	 * @param metric
	 *            string with name of the monitored metric
	 * @return a string with the relevant rule key
	 */
	public String getMostRelevantRule(String group, String host, String metric) {
		if (!this.metrics.contains(metric)) {
			return ""; // nothing found
		} else {

			// order or precedence: more specific first
			// group,host,metric #1
			// ,host,metric #2
			// group ,metric #3
			// ,metric #4
			if (this.hosts.contains(host)) {
				if (this.groups.contains(group)) {
					// check if combined entry indeed exists
					String key = String.format("%s/%s/%s", group, host, metric);
					if (this.rules.containsKey(key))
						return key; // #1

				} else {
					return String.format("/%s/%s", host, metric); // #2
				}
			}

			if (this.groups.contains(group)) {
				// check if combined entry indeed exists
				String key = String.format("%s//%s", group, metric); // #3
				if (this.rules.containsKey(key))
					return key;
			}

			return String.format("//%s", metric);
		}

	}

	/**
	 * Parses an expression that might contain multiple labels=thresholds separated
	 * by whitespace and creates a HashMap of labels to parsed threshold objects
	 * 
	 * @param thresholds
	 *            an expression that might contain multiple thresholds
	 * @return a HashMap to Threshold objects
	 */
	public Map<String, Threshold> parseThresholds(String thresholds) {
		Map<String, Threshold> subMap = new HashMap<String, Threshold>();
		// Tokenize with lookahead on the point when a new label starts
		String[] tokens = thresholds.split("(;|[ ]+)(?=[a-zA-Z])");
		for (String token : tokens) {
			Threshold curTh = new Threshold(token);
			if (curTh != null) {
				subMap.put(curTh.getLabel(), curTh);
			}
		}
		return subMap;
	}

	/**
	 * Parses an expression that might contain multiple labels=thresholds separated
	 * by whitespace and creates a HashMap of labels to parsed Float values
	 * 
	 * @param thresholds
	 *            an expression that might contain multiple thresholds
	 * @return a HashMap to Floats
	 */
	public Map<String, Float> getThresholdValues(String thresholds) {
		Map<String, Float> subMap = new HashMap<String, Float>();
		// tokenize thresholds by whitespace
		String[] tokens = thresholds.split("(;|[ ]+)(?=[a-zA-Z])");
		for (String token : tokens) {
			Threshold curTh = new Threshold(token);
			if (curTh != null) {
				subMap.put(curTh.getLabel(), curTh.getValue());
			}
		}
		return subMap;
	}

	/**
	 * Parses a JSON threshold rule file and populates the ThresholdManager
	 * 
	 * @param jsonFile
	 *            File to be parsed
	 * @return boolean signaling whether operation succeeded or not
	 */
	public boolean parseJSONFile(File jsonFile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(jsonFile));
			String jsonStr = IOUtils.toString(br);
			if (!parseJSON(jsonStr))
				return false;

		} catch (IOException ex) {
			LOG.error("Could not open file:" + jsonFile.getName());
			return false;

		} catch (JsonParseException ex) {
			LOG.error("File is not valid json:" + jsonFile.getName());
			return false;
		} finally {
			// Close quietly without exceptions the buffered reader
			IOUtils.closeQuietly(br);
		}

		return true;

	}

	/**
	 * Parses a json string with the appropriate threshold rule schema and populates
	 * the ThresholdManager
	 * 
	 * @param jsonString
	 *            string containing threshold rules in json format
	 * @return boolean signaling whether the parse information succeded or not
	 */
	public boolean parseJSON(String jsonString) {
		

		JsonParser json_parser = new JsonParser();
		JsonObject jRoot = json_parser.parse(jsonString).getAsJsonObject();
		JsonArray jRules = jRoot.getAsJsonArray("rules");
		for (JsonElement jRule : jRules) {
			JsonObject jRuleObj = jRule.getAsJsonObject();
			String ruleMetric = jRuleObj.getAsJsonPrimitive("metric").getAsString();
			String ruleHost = "";
			String ruleEgroup = "";

			if (jRuleObj.has("host")) {
				ruleHost = jRuleObj.getAsJsonPrimitive("host").getAsString();
			}
			if (jRuleObj.has("endpoint_group")) {
				ruleEgroup = jRuleObj.getAsJsonPrimitive("endpoint_group").getAsString();
			}

			String ruleThr = jRuleObj.getAsJsonPrimitive("thresholds").getAsString();
			this.metrics.add(ruleMetric);
			if (ruleHost != "")
				this.hosts.add(ruleHost);
			if (ruleEgroup != "")
				this.groups.add(ruleEgroup);
			String full = ruleEgroup + "/" + ruleHost + "/" + ruleMetric;
			Map<String, Threshold> thrMap = parseThresholds(ruleThr);
			this.rules.put(full, thrMap);
		}

		return true;
	}

}
