package argo.amr;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;



import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricProfile;
import argo.avro.Weight;


/**
 * APIResourceManager class fetches remote argo-web-api resources such as
 * report configuration, profiles, topology, weights in JSON format
 */


public class ApiResourceManager { 
	
	private EnumMap<ApiResource, String> data = new EnumMap<>(ApiResource.class);

	private String endpoint;
	private String token;
	private String reportID;
	private String date;
	private String proxy;

	private String metricID;
	private String aggregationID;
	private String opsID;
	private String threshID;
	private String reportName;
	private String weightsID;
	private boolean verify;


	public ApiResourceManager(String endpoint, String token) {
		this.endpoint = endpoint;
		this.token = token;
		this.metricID = "";
		this.aggregationID = "";
		this.opsID = "";
		this.threshID = "";
		this.reportName = "";
		this.reportID = "";
		this.date = "";
		this.proxy = "";
		this.weightsID = "";
		this.verify = true;

	}
	
	public boolean getVerify() {
		return verify;
	}
	
	public void setVerify(boolean verify) {
		this.verify = verify;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getReportID() {
		return reportID;
	}

	public void setReportID(String reportID) {
		this.reportID = reportID;
	}
	
	public String getReportName() {
		return this.reportName;
	}
	
	public String getOpsID() {
		return this.opsID;
	}
	
	
	public String getAggregationID() {
		return this.aggregationID;
	}
	
	public String getMetricID() {
		return this.metricID;
	}
	
	public String getThresholdsID() {
		return this.threshID;
	}
	

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getProxy() {
		return proxy;
	}

	public void setProxy(String proxy) {
		this.proxy = proxy;
	}

	public String getWeightsID() {
		return weightsID;
	}

	public void setWeightsID(String weightsID) {
		this.weightsID = weightsID;
	}
	
	/**
	 * Create an SSL Connection Socket Factory with a strategy to trust self signed
	 * certificates
	 */
	private SSLConnectionSocketFactory selfSignedSSLF()
			throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
		SSLContextBuilder sslBuild = new SSLContextBuilder();
		sslBuild.loadTrustMaterial(null, new TrustSelfSignedStrategy());
		return new SSLConnectionSocketFactory(sslBuild.build(), NoopHostnameVerifier.INSTANCE);
	}

	/**
	 * Contacts remote argo-web-api based on the full url of a resource  its content (expected in json format)
	 * 
	 * @param fullURL String containing the full url representation of the argo-web-api resource
	 * @return A string representation of the resource json content
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	private String getResource(String fullURL) {
		
		
		Request r = Request.Get(fullURL).addHeader("Accept", "application/json").addHeader("Content-type",
				"application/json").addHeader("x-api-key",this.token);
		if (!this.proxy.isEmpty()) {
			r = r.viaProxy(proxy);
		}
		
		r = r.connectTimeout(1000).socketTimeout(1000);
		
		String content = "{}";
		
		try {
			if (this.verify == false) {
				CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(selfSignedSSLF()).build();
				Executor executor = Executor.newInstance(httpClient);
				content = executor.execute(r).returnContent().asString();
			} else {
				
				content = r.execute().returnContent().asString();
			}
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return content;
	}

	/**
	 * Retrieves the remote report configuration based on reportID main class attribute and 
	 * stores the content in the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteConfig() {
		String path = "https://%s/api/v2/reports/%s";
		String fullURL = String.format(path, this.endpoint, this.reportID);
		String content = getResource(fullURL);
		this.data.put(ApiResource.CONFIG, getJsonData(content, false));
	}

	
	/**
	 * Retrieves the metric profile content based on the metric_id attribute and stores it to the enum map
	 *  
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteMetric()  {

		String path = "https://%s/api/v2/metric_profiles/%s?date=%s";
		String fullURL = String.format(path, this.endpoint, this.metricID, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.METRIC, getJsonData(content, false));
	}

	/**
	 * Retrieves the aggregation profile content based on the aggreagation_id attribute and stores it to the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteAggregation()  {

		String path = "https://%s/api/v2/aggregation_profiles/%s?date=%s";
		String fullURL = String.format(path, this.endpoint, this.aggregationID, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.AGGREGATION, getJsonData(content, false));
	}

	/**
	 * Retrieves the ops profile content based on the ops_id attribute and stores it to the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteOps()  {

		String path = "https://%s/api/v2/operations_profiles/%s?date=%s";
		String fullURL = String.format(path, this.endpoint, this.opsID, this.date);
		
		String content = getResource(fullURL);
		this.data.put(ApiResource.OPS, getJsonData(content, false));
	}

	/**
	 * Retrieves the thresholds profile content based on the thresh_id attribute and stores it to the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteThresholds() {
		
		String path = "https://%s/api/v2/thresholds_profiles/%s?date=%s";
		String fullURL = String.format(path, this.endpoint, this.threshID, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.THRESHOLDS, getJsonData(content, false));
	}

	/**
	 * Retrieves  the topology endpoint content and stores it to the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteTopoEndpoints()  {
		String path = "https://%s/api/v2/topology/endpoints/by_report/%s?date=%s";
		String fullURL = String.format(path, this.endpoint, this.reportName, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.TOPOENDPOINTS, getJsonData(content, true));
	}

	/**
	 * Retrieves the topology groups content and stores it to the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteTopoGroups()  {
		String path = "https://%s/api/v2/topology/groups/by_report/%s?date=%s";
		String fullURL = String.format(path, this.endpoint, this.reportName, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.TOPOGROUPS, getJsonData(content, true));
	}

	/**
	 * Retrieves the weights content and stores it to the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteWeights()  {
		String path = "https://%s/api/v2/weights/%s?date=%s";
		String fullURL = String.format(path, this.endpoint, this.weightsID, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.WEIGHTS, getJsonData(content, false));
	}

	/**
	 * Retrieves the downtimes content and stores it to the enum map
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteDowntimes()  {
		String path = "https://%s/api/v2/downtimes?date=%s";
		String fullURL = String.format(path, this.endpoint, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.DOWNTIMES, getJsonData(content, false));
	}
	
	public void getRemoteRecomputations()  {
		String path = "https://%s/api/v2/recomputations?date=%s";
		String fullURL = String.format(path, this.endpoint, this.date);
		String content = getResource(fullURL);
		this.data.put(ApiResource.RECOMPUTATIONS, getJsonData(content, true));
	}
	
	/**
	 * Returns local resource (after has been retrieved) content based on resource type
	 * 
	 * @param res
	 * @return The extracted items JSON value as string
	 */
	public String getResourceJSON(ApiResource res) {
		return this.data.get(res);
	}

	/**
	 * Exectues all steps to retrieve the complete amount of the available profile,
	 * topology, weights and downtime information from argo-web-api
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 * @throws KeyStoreException 
	 * @throws NoSuchAlgorithmException 
	 * @throws KeyManagementException 
	 */
	public void getRemoteAll()  {
		// Start with report and configuration
		this.getRemoteConfig();
		// parse remote report config to be able to get the other profiles
		this.parseReport();
		// Go on to the profiles
		this.getRemoteMetric();
		this.getRemoteOps();
		this.getRemoteAggregation();
		if (!this.threshID.equals(""))	this.getRemoteThresholds();
		// Go to topology
		this.getRemoteTopoEndpoints();
		this.getRemoteTopoGroups();
		// get weights
		if (!this.weightsID.equals(""))	this.getRemoteWeights();
		// get downtimes
		this.getRemoteDowntimes();
		// get recomptations
		this.getRemoteRecomputations();

	}

	/**
	 * Parses the report content to extract the report's name and the various profile IDs
	 */
	public void parseReport() {
		// check if report configuration has been retrieved
		if (!this.data.containsKey(ApiResource.CONFIG))
			return;

		String content = this.data.get(ApiResource.CONFIG);
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonObject jRoot = jElement.getAsJsonObject();
		JsonArray jProfiles = jRoot.get("profiles").getAsJsonArray();

		JsonObject jInfo = jRoot.get("info").getAsJsonObject();
		this.reportName = jInfo.get("name").getAsString();

		// for each profile iterate and store it's id in profile manager for later
		// reference
		for (int i = 0; i < jProfiles.size(); i++) {
			JsonObject jProf = jProfiles.get(i).getAsJsonObject();
			String profType = jProf.get("type").getAsString();
			String profID = jProf.get("id").getAsString();
			if (profType.equalsIgnoreCase("metric")) {
				this.metricID = profID;
			} else if (profType.equalsIgnoreCase("aggregation")) {
				this.aggregationID = profID;
			} else if (profType.equalsIgnoreCase("operations")) {
				this.opsID = profID;
			} else if (profType.equalsIgnoreCase("thresholds")) {
				this.threshID = profID;
			}

		}

	}
	
	/**
	 * Parses the Downtime content retrieved from argo-web-api and provides a list of Downtime avro objects
	 * to be used in the next steps of the pipeline
	 */
	public Downtime[] getListDowntimes() {
		List<Downtime> results = new ArrayList<Downtime>();
		if (!this.data.containsKey(ApiResource.DOWNTIMES)) {
			Downtime[] rArr = new Downtime[results.size()];
			rArr = results.toArray(rArr);
		}
			
		
		String content = this.data.get(ApiResource.DOWNTIMES);
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonObject jRoot = jElement.getAsJsonObject();
		JsonArray jElements = jRoot.get("endpoints").getAsJsonArray();
		for (int i = 0; i < jElements.size(); i++) {
			JsonObject jItem= jElements.get(i).getAsJsonObject();
			String hostname = jItem.get("hostname").getAsString();
			String service = jItem.get("service").getAsString();
			String startTime = jItem.get("start_time").getAsString();
			String endTime = jItem.get("end_time").getAsString();
			
			Downtime d = new Downtime(hostname,service,startTime,endTime);
			results.add(d);
		}
		
		Downtime[] rArr = new Downtime[results.size()];
		rArr = results.toArray(rArr);
		return rArr;
	}

	/**
	 * Parses the Topology endpoint content retrieved from argo-web-api and provides a list of GroupEndpoint avro objects
	 * to be used in the next steps of the pipeline
	 */
	public GroupEndpoint[] getListGroupEndpoints() {
		List<GroupEndpoint> results = new ArrayList<GroupEndpoint>();
		if (!this.data.containsKey(ApiResource.TOPOENDPOINTS)) {
			GroupEndpoint[] rArr = new GroupEndpoint[results.size()]; 
			rArr = results.toArray(rArr);
			return rArr;
		}
			
		
		String content = this.data.get(ApiResource.TOPOENDPOINTS);
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonArray jRoot = jElement.getAsJsonArray();
		for (int i = 0; i < jRoot.size(); i++) {
			JsonObject jItem= jRoot.get(i).getAsJsonObject();
			String group = jItem.get("group").getAsString();
			String gType = jItem.get("type").getAsString();
			String service = jItem.get("service").getAsString();
			String hostname = jItem.get("hostname").getAsString();
			JsonObject jTags = jItem.get("tags").getAsJsonObject();
			Map<String,String> tags = new HashMap<String,String>();
		    for ( Entry<String, JsonElement> kv : jTags.entrySet()) {
		    	tags.put(kv.getKey(), kv.getValue().getAsString());
		    }
			GroupEndpoint ge = new GroupEndpoint(gType,group,service,hostname,tags);
			results.add(ge);
		}
		
		GroupEndpoint[] rArr = new GroupEndpoint[results.size()]; 
		rArr = results.toArray(rArr);
		return rArr;
	}
	
	/**
	 * Parses the Topology Groups content retrieved from argo-web-api and provides a list of GroupGroup avro objects
	 * to be used in the next steps of the pipeline
	 */
	public GroupGroup[] getListGroupGroups() {
		List<GroupGroup> results = new ArrayList<GroupGroup>();
		if (!this.data.containsKey(ApiResource.TOPOGROUPS)){
			GroupGroup[] rArr = new GroupGroup[results.size()]; 
			rArr = results.toArray(rArr);
			return rArr;
		}
		
		String content = this.data.get(ApiResource.TOPOGROUPS);
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonArray jRoot = jElement.getAsJsonArray();
		for (int i = 0; i < jRoot.size(); i++) {
			JsonObject jItem= jRoot.get(i).getAsJsonObject();
			String group = jItem.get("group").getAsString();
			String gType = jItem.get("type").getAsString();
			String subgroup = jItem.get("subgroup").getAsString();
			JsonObject jTags = jItem.get("tags").getAsJsonObject();
			Map<String,String> tags = new HashMap<String,String>();
		    for ( Entry<String, JsonElement> kv : jTags.entrySet()) {
		    	tags.put(kv.getKey(), kv.getValue().getAsString());
		    }
			GroupGroup gg = new GroupGroup(gType,group,subgroup,tags);
			results.add(gg);
		}
		
		GroupGroup[] rArr = new GroupGroup[results.size()]; 
		rArr = results.toArray(rArr);
		return rArr;
	}
	
	/**
	 * Parses the Weights content retrieved from argo-web-api and provides a list of Weights avro objects
	 * to be used in the next steps of the pipeline
	 */
	public Weight[] getListWeights() {
		List<Weight> results = new ArrayList<Weight>();
		if (!this.data.containsKey(ApiResource.WEIGHTS)) {
			Weight[] rArr = new Weight[results.size()]; 
			rArr = results.toArray(rArr);
			return rArr;
		}
			
		
		String content = this.data.get(ApiResource.WEIGHTS);
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonObject jRoot = jElement.getAsJsonObject();
		String wType = jRoot.get("weight_type").getAsString();
		JsonArray jElements = jRoot.get("groups").getAsJsonArray();
		for (int i = 0; i < jElements.size(); i++) {
			JsonObject jItem= jElements.get(i).getAsJsonObject();
			String group = jItem.get("name").getAsString();
			String weight = jItem.get("value").getAsString();
			
			Weight w = new Weight(wType,group,weight);
			results.add(w);
		}
		
		Weight[] rArr = new Weight[results.size()]; 
		rArr = results.toArray(rArr);
		return rArr;
	}
	
	/**
	 * Parses the Metric profile content retrieved from argo-web-api and provides a list of MetricProfile avro objects
	 * to be used in the next steps of the pipeline
	 */
	public MetricProfile[] getListMetrics() {
		List<MetricProfile> results = new ArrayList<MetricProfile>();
		if (!this.data.containsKey(ApiResource.METRIC)) {
			MetricProfile[] rArr = new MetricProfile[results.size()]; 
			rArr = results.toArray(rArr);
			return rArr;
		}
			
		
		String content = this.data.get(ApiResource.METRIC);
		JsonParser jsonParser = new JsonParser();
		JsonElement jElement = jsonParser.parse(content);
		JsonObject jRoot = jElement.getAsJsonObject();
		String profileName = jRoot.get("name").getAsString();
		JsonArray jElements = jRoot.get("services").getAsJsonArray();
		for (int i = 0; i < jElements.size(); i++) {
			JsonObject jItem= jElements.get(i).getAsJsonObject();
			String service = jItem.get("service").getAsString();
			JsonArray jMetrics = jItem.get("metrics").getAsJsonArray();
			for (int j=0; j < jMetrics.size(); j++) {
				String metric = jMetrics.get(j).getAsString();
				
				Map<String,String> tags = new HashMap<String,String>();
				MetricProfile mp = new MetricProfile(profileName,service,metric,tags);
				results.add(mp);
			}
			
		}
		
		MetricProfile[] rArr = new MetricProfile[results.size()]; 
		rArr = results.toArray(rArr);
		return rArr;
	}

	/**
	 * Extract first JSON item from data JSON array in api response
	 *  
	 * @param content JSON content of the full repsonse (status + data)
	 * @return First available item in data array as JSON string representation
	 * 
	 */
	private String getJsonData(String content, boolean asArray) {
		JsonParser jsonParser = new JsonParser();
		// Grab the first - and only line of json from ops data
		JsonElement jElement = jsonParser.parse(content);
		JsonObject jRoot = jElement.getAsJsonObject();
		// Get the data array and the first item
		if (asArray) {
			return jRoot.get("data").toString();
		}
		JsonArray jData = jRoot.get("data").getAsJsonArray();
		JsonElement jItem = jData.get(0);
		return jItem.toString();
	}

}
