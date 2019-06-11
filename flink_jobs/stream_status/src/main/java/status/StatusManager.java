package status;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

import sync.AggregationProfileManager;
import sync.DowntimeCache;
import sync.EndpointGroupManagerV2;
import sync.EndpointGroupManagerV2.EndpointItem;
import sync.MetricProfileManager;
import ops.OpsManager;

import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;

import argo.avro.Downtime;
import argo.avro.GroupEndpoint;
import argo.avro.MetricProfile;


/**
 * Status Manager implements a live structure containing a topology of entities
 * and the related statuses for each one
 */
public class StatusManager {

	// Initialize logger
	static Logger LOG = LoggerFactory.getLogger(StatusManager.class);

	// Name of the report used
	private String report;

	// Sync file structures necessary for status computation
	public EndpointGroupManagerV2 egp = new EndpointGroupManagerV2();
	public MetricProfileManager mps = new MetricProfileManager();
	AggregationProfileManager aps = new AggregationProfileManager();
	OpsManager ops = new OpsManager();
	private Long timeout = 86400000L;
	
	// Add downtime manager cache - 5 slots are enough for status manager case
	private DowntimeCache dc = new DowntimeCache(5);

	// Names of valid profiles and services used
	String validMetricProfile;
	String validAggProfile;
	ArrayList<String> validServices = new ArrayList<String>();

	// Structure to hold topology entities and related statuses
	Map<String, StatusNode> groups = new HashMap<String, StatusNode>();

	// Flag used in initial event generation
	Boolean firstGen = true;

	// Timestamp of the latest processed event used as a daily event generation
	// trigger
	String tsLatest;
	
	public void setReport(String report) {
		this.report = report;
	}
	
	public String getReport() {
		return this.report;
	}
	
	public void setTimeout(Long timeout) {
		this.timeout = timeout;
	}
	
	public Long getTimeout() {
		return this.timeout;
	}

	// Get Operation Manager
	public OpsManager getOps() {
		return this.ops;
	}

	/**
	 * Status Item is a simple structure holding the latest status for an entity
	 * along with the timestamp of the event
	 */
	public class StatusItem {
		int status;
		Date timestamp;
		Date genTs;
	}
	
	
	public void addDowntimeSet(String dayStamp, ArrayList<Downtime> downList) {
		this.dc.addFeed(dayStamp, downList);
	}

	/**
	 * Status Node represents status information for entity in the topology tree An
	 * entity might contain other entities with status information
	 */
	public class StatusNode {
		// Type of entity: endpoint_group,service,endpoint or metric
		String type;
		// Status information with timestamp
		StatusItem item;
		// A list of entities contained as children
		Map<String, StatusNode> children = new HashMap<String, StatusNode>();
		// Reference to the parent node
		StatusNode parent = null;

		/**
		 * Creates a new status node
		 * 
		 * @param type
		 *            A string containing the node type
		 *            (endpoint_group,service,endpoint,metric)
		 * @param defStatus
		 *            Default status value
		 * @param defTs
		 *            Default timestamp
		 */
		public StatusNode(String type, int defStatus, Date defTs) {
			this.type = type;
			this.item = new StatusItem();
			this.item.status = defStatus;
			this.item.timestamp = defTs;
			this.item.genTs = defTs;
			this.parent = null;
		}

		/**
		 * Creates a new status node given a parent
		 * 
		 * @param type
		 *            A string containing the node type
		 *            (endpoint_group,service,endpoint,metric)
		 * @param defStatus
		 *            Default status value
		 * @param defTs
		 *            Default timestamp
		 * @param parent
		 *            Reference to the parent status node
		 */
		public StatusNode(String type, int defStatus, Date defTs, StatusNode parent) {
			this.type = type;
			this.item = new StatusItem();
			this.item.status = defStatus;
			this.item.timestamp = defTs;
			this.parent = parent;
		}

	}
	
	
	public void removeEndpoint(String endpointDef) {
		String[] tokens = endpointDef.split(",");
		if (tokens.length != 4) return; //endpoint definition must split to 4 tokens
		String etype = tokens[0];
		String group = tokens[1];
		String service = tokens[2];
		String hostname = tokens[3];
	
		if (!this.groups.containsKey(group)) return;
		StatusNode groupNode = this.groups.get(group);
		
		if (!groupNode.children.containsKey(service)) return;
		StatusNode serviceNode = groupNode.children.get(service);
		
		if (!serviceNode.children.containsKey(hostname)) return;
				
		// Remove endpoint 
		serviceNode.children.remove(hostname);
		Log.info("Removed endpoint:" + hostname + " from the tree");
		// if service node contains other items return
		if (!serviceNode.children.isEmpty()) return;
		groupNode.children.remove(service);
		Log.info("Removed service:" + service + " from the tree");
		
		// if group node contains other items return
		if (!groupNode.children.isEmpty()) return;
		this.groups.remove(group);
		Log.info("Removed group:" + group + " from the tree");
		
	
	}
	
	public void updateTopology(EndpointGroupManagerV2 egpNext) {
		// find a list of lost items to remove them from status tree
		ArrayList<String> lostItems = this.egp.compareToBeRemoved(egpNext);
		
		for (String item : lostItems) {
			removeEndpoint(item);
		}
		
	}

	public boolean hasEndpoint(String group, String service, String hostname) {
		if (hasService(group, service)) {
			StatusNode groupNode = this.groups.get(group);
			StatusNode serviceNode = groupNode.children.get(service);
			return serviceNode.children.containsKey(hostname);
		}
		
		return false;
	}
	
	/**
	 * Checks if this status manager handles the specific endpoint group service
	 */
	public boolean hasService(String group, String service) {
		if (hasGroup(group)) {
			StatusNode groupNode = this.groups.get(group);
			return groupNode.children.containsKey(service);
		}
		
		return false;
	}
	
	/**
	 * Checks if this status manager handles the specific endpoint group
	 */
	public boolean hasGroup(String group) {
		return this.groups.containsKey(group);
	}

	/**
	 * Set the latest processed timestamp value
	 */
	public void setTsLatest(String ts) {
		this.tsLatest = ts;
	}

	/**
	 * Get the latest processed timestamp value
	 */
	public String getTsLatest() {
		return this.tsLatest;
	}

	/**
	 * Disable flag for initial event generation
	 */
	public void disableFirstGen() {
		this.firstGen = false;
	}

	/**
	 * Check if day has changed between two sequential timestamps
	 * 
	 * @param tsOld
	 *            Previous timestamp
	 * @param tsNew
	 *            Newest timestamp
	 */
	public boolean hasDayChanged(String tsOld, String tsNew) {
		if (tsOld == null)
			return false;


		String dtOld = tsOld.split("T")[0];
		String dtNew = tsNew.split("T")[0];

		if (dtOld.compareToIgnoreCase(dtNew) != 0) {
			return true;
		}

		return false;
	}

	/**
	 * Get firstGen parameter flag to check if initial event generation is needed
	 */
	public boolean getFirstGen() {
		return this.firstGen;
	}

	/**
	 * Get today's datetime at the beginning of day
	 * 
	 * @return Date at the beginning of day
	 */
	public Date getToday() {
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTime();
	}

	/**
	 * Convert zulu timestamp in date object
	 * 
	 * @param zulu
	 *            String representing a zulu timestamp
	 * @return Date object
	 */
	public Date setDate(String zulu) throws ParseException {
		String[] parts = zulu.split("T");
		return fromZulu(parts[0] + "T00:00:00Z");
	}

	/**
	 * Compare profiles for validity and extract valid services
	 */
	public void setValidProfileServices() {
		// Get services from first profile
		this.validMetricProfile = this.mps.getProfiles().get(0);
		this.validAggProfile = this.aps.getAvProfiles().get(0);
		this.validServices = this.mps.getProfileServices(this.validMetricProfile);
	}

	/**
	 * Load all initial Profiles from object lists
	 * 
	 * @param egpAvro
	 *            endpoint group object list
	 * @param mpsAvro
	 *            metric profile object list
	 * @param apsJson
	 *            aggregation profile contents
	 * @param opsJson
	 *            operation profile contents
	 */
	public void loadAll(String runDate, ArrayList<Downtime> downList, ArrayList<GroupEndpoint> egpList, ArrayList<MetricProfile> mpsList, String apsJson,
			String opsJson) throws IOException {
		aps.loadJsonString(apsJson);
		ops.loadJsonString(opsJson);
		mps.loadFromList(mpsList);
		
		// First downtime loaded in cache 
		dc.addFeed(runDate, downList);
		
		
		setValidProfileServices();
		// Trim endpoint group list based on metric profile information (remove unwanted
		// services)

		ArrayList<GroupEndpoint> egpTrim = new ArrayList<GroupEndpoint>();

		for (GroupEndpoint egpItem : egpList) {
			if (validServices.contains(egpItem.getService())) {
				egpTrim.add(egpItem);
			}
		}

		egp.loadFromList(egpTrim);

	}

	/**
	 * Load all initial Profiles directly from files
	 * 
	 * @param egpAvro
	 *            endpoint group topology location
	 * @param mpsAvro
	 *            metric profile location
	 * @param apsJson
	 *            aggregation profile location
	 * @param opsJson
	 *            operation profile location
	 */
	public void loadAllFiles(String dayStamp, File downAvro, File egpAvro, File mpsAvro, File apsJson, File opsJson) throws IOException {
		dc.addFileFeed(dayStamp, downAvro);
		egp.loadAvro(egpAvro);
		mps.loadAvro(mpsAvro);
		aps.loadJson(apsJson);
		ops.loadJson(opsJson);

		setValidProfileServices();
	}

	/**
	 * Construct status topology with initial status value and timestamp
	 * 
	 * @param defStatus
	 *            Initial status to be used
	 * @param defTs
	 *            Initial timestamp to be used
	 */
	public void addNewGroup(String group, int defStatus, Date defTs) {
		// Get all the available group's hosts
		Iterator<EndpointItem> hostIter = egp.getGroupIter(group);
		// for each host in specific group iterate
		while (hostIter.hasNext()) {
			EndpointItem host = hostIter.next();
			String service = host.getService();
			String hostname = host.getHostname();

			if (this.validServices.contains(service)) {
				// Add host to groups
				addGroup(group, service, hostname, defStatus, defTs);
			}

		}
	}

	/**
	 * Add a new endpoint group node to the status topology using metric data
	 * information
	 * 
	 * @param group
	 *            Name of the endpoint group
	 * @param service
	 *            Name of the service flavor
	 * @param hostname
	 *            Name of the endpoint
	 * @param defStatus
	 *            Default status to be initialized to
	 * @param defTs
	 *            Default timestamp to be initialized to
	 */
	public void addGroup(String group, String service, String hostname, int defStatus, Date defTs) {
		// Check if group exists
		if (!this.groups.containsKey(group)) {
			StatusNode groupNode = new StatusNode("group", defStatus, defTs);
			this.groups.put(group, groupNode);
			// Add to the new node
			addService(groupNode, service, hostname, defStatus, defTs);
			return;
		}

		// Find group node and continue adding service under there
		addService(this.groups.get(group), service, hostname, defStatus, defTs);

	}

	/**
	 * Add a new service node to the status topology using metric data information
	 * 
	 * @param groupNode
	 *            Reference to the parent node
	 * @param service
	 *            Name of the service flavor
	 * @param hostname
	 *            Name of the endpoint
	 * @param defStatus
	 *            Default status to be initialized to
	 * @param defTs
	 *            Default timestamp to be initialized to
	 */
	public void addService(StatusNode groupNode, String service, String hostname, int defStatus, Date defTs) {
		if (!groupNode.children.containsKey(service)) {
			StatusNode serviceNode = new StatusNode("service", defStatus, defTs, groupNode);
			groupNode.children.put(service, serviceNode);
			// Add to the new node
			addEndpoint(serviceNode, service, hostname, defStatus, defTs);
			return;
		}

		// Find service node and continue adding endpoint under there
		addEndpoint(groupNode.children.get(service), service, hostname, defStatus, defTs);
	}

	/**
	 * Add a new endpoint node to the status topology using metric data information
	 * 
	 * @param serviceNode
	 *            Reference to the parent node
	 * @param service
	 *            Name of the service flavor
	 * @param hostname
	 *            Name of the endpoint
	 * @param defStatus
	 *            Default status to be initialized to
	 * @param defTs
	 *            Default timestamp to be initialized to
	 */
	public void addEndpoint(StatusNode serviceNode, String service, String hostname, int defStatus, Date defTs) {
		if (!serviceNode.children.containsKey(hostname)) {
			StatusNode endpointNode = new StatusNode("endpoint", defStatus, defTs, serviceNode);
			serviceNode.children.put(hostname, endpointNode);
			// Add to the new node
			addMetrics(endpointNode, service, hostname, defStatus, defTs);
			return;
		}

		// Find endpoint node and continue adding metrics under there
		addMetrics(serviceNode.children.get(hostname), service, hostname, defStatus, defTs);
	}

	/**
	 * Add a new metrics node to the status topology using metric data information
	 * 
	 * @param endpointNode
	 *            Reference to the parent node
	 * @param service
	 *            Name of the service flavor
	 * @param hostname
	 *            Name of the endpoint
	 * @param defStatus
	 *            Default status to be initialized to
	 * @param defTs
	 *            Default timestamp to be initialized to
	 */
	public void addMetrics(StatusNode endpointNode, String service, String hostname, int defStatus, Date defTs) {
		ArrayList<String> metrics = this.mps.getProfileServiceMetrics(this.validMetricProfile, service);

		// Check if metrics = null
		if (metrics == null) {
			String msg = endpointNode + "/" + service + "/" + hostname + " " + this.validMetricProfile;
			throw new RuntimeException(msg);
		}

		// For all available metrics create leaf metric nodes
		for (String metric : metrics) {
			StatusNode metricNode = new StatusNode("metric", defStatus, defTs, endpointNode);
			metricNode.children = null;
			endpointNode.children.put(metric, metricNode);
		}
	}

	/**
	 * Convert a timestamp string to date object
	 * 
	 * @param zulu
	 *            String with timestamp in zulu format
	 * @return Date object
	 */
	public Date fromZulu(String zulu) throws ParseException {
		DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = utcFormat.parse(zulu);
		return date;
	}

	/**
	 * Convert a date object to a string timestamp in zulu format
	 * 
	 * @param ts
	 *            Date object to be converted
	 * @return String with timestamp in zulu format
	 */
	public String toZulu(Date ts) throws ParseException {
		DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return utcFormat.format(ts);
	}
	
	
	
	

	/**
	 * For all entities in the topology generate status events
	 * 
	 * @param tsStr
	 *            String containing timestamp of status generation
	 * @return List of generated events in string json format
	 */
	public ArrayList<String> dumpStatus(String tsStr) throws ParseException {
		// Convert timestamp to date object
		Date ts = fromZulu(tsStr);
		// Initialize event list
		ArrayList<String> results = new ArrayList<String>();
		
		StatusEvent evtMetric = new StatusEvent();
		StatusEvent evtEndpoint = new StatusEvent();
		StatusEvent evtService = new StatusEvent();
		StatusEvent evtEgroup = new StatusEvent();
		
		
		String[] statusMetric = new String[4];
		String[] statusEndpoint = new String[4];
		String[] statusService = new String[4];
		String[] statusEgroup = new String[4];
		

		// For each endpoint group in topology
		for (String groupName : groups.keySet()) {
			StatusNode groupNode = groups.get(groupName);
			String groupStatus = ops.getStrStatus(groupNode.item.status);
			Date groupTs = groupNode.item.timestamp;

			// For each service in the specific endpoint group
			for (String serviceName : groupNode.children.keySet()) {
				StatusNode serviceNode = groupNode.children.get(serviceName);
				String serviceStatus = ops.getStrStatus(serviceNode.item.status);
				Date serviceTs = serviceNode.item.timestamp;

				// For each endpoint in the specific service
				for (String endpointName : serviceNode.children.keySet()) {
					StatusNode endpointNode = serviceNode.children.get(endpointName);
					String endpointStatus = ops.getStrStatus(endpointNode.item.status);
					Date endpointTs = endpointNode.item.timestamp;

					// For each metric in the specific service endpoint
					for (String metricName : endpointNode.children.keySet()) {
						StatusNode metricNode = endpointNode.children.get(metricName);
						String metricStatus = ops.getStrStatus(metricNode.item.status);
						Date metricTs = metricNode.item.timestamp;
						// Generate metric status event
						evtMetric = genEvent("metric", groupName, serviceName, endpointName, metricName, metricStatus,
								"", metricTs, metricStatus, metricTs, true,"","");
						
						statusMetric = new String[]{evtMetric.getStatus(),evtMetric.getPrevStatus(),evtMetric.getTsProcessed(),evtMetric.getPrevTs()};
						evtMetric.setStatusMetric(statusMetric);
						results.add(eventToString(evtMetric));
						
						
					}
					// Generate endpoint status event
					evtEndpoint = genEvent("endpoint", groupName, serviceName, endpointName, "", endpointStatus, "", ts,
							endpointStatus, endpointTs, true,"","");
					
					statusEndpoint = new String[] {evtEndpoint.getStatus(),evtEndpoint.getPrevStatus(),evtEndpoint.getTsMonitored(),evtEndpoint.getPrevTs()};
					evtEndpoint.setStatusMetric(statusMetric);
					evtEndpoint.setStatusEndpoint(statusEndpoint);
					
					
					results.add(eventToString(evtEndpoint));
				}
				// Generate service status event
				evtService = genEvent("service", groupName, serviceName, "", "", serviceStatus, "", ts, serviceStatus,
						serviceTs, true,"","");
				
				statusService = new String[] {evtService.getStatus(),evtService.getPrevStatus(),evtService.getTsMonitored(),evtService.getPrevTs()};
				evtService.setStatusMetric(statusMetric);
				evtService.setStatusEndpoint(statusEndpoint);
				evtService.setStatusService(statusService);
				
				results.add(eventToString(evtService));
			}
			// Generate endpoint group status event
			evtEgroup = genEvent("grpoup", groupName, "", "", "", groupStatus, "", ts, groupStatus, groupTs, true,"","");
			statusEgroup = new String[] {evtEgroup.getStatus(),evtEgroup.getPrevStatus(),evtEgroup.getTsMonitored(),evtEgroup.getPrevTs()};
			evtEgroup.setStatusMetric(statusMetric);
			evtEgroup.setStatusEndpoint(statusEndpoint);
			evtEgroup.setStatusService(statusService);
			evtEgroup.setStatusEgroup(statusEgroup);
			
			results.add(eventToString(evtEgroup));			
			
		}

		return results;
	}
	
	public boolean hasTimeDiff(Date d1, Date d2, long timeout) {
		if (d2 == null || d1 == null) {
			return false;
		}
		
		Long diff = d1.getTime() - d2.getTime();
		
		if (diff >= timeout) {
			LOG.debug("Will regenerate event -time passed (hours):" + diff/3600000);
			return true;
		}
		
		return false;
		
	}
	
	public boolean hasDowntime(String timestamp, String hostname, String service ) {
		String dayStamp = timestamp.split("T")[0];
		ArrayList<String> period = this.dc.getDowntimePeriod(dayStamp, hostname, service);
		// if no period was found return immediately fals
		if (period == null) return false;
		
		// else check if ts lower than period's start time (first element in array list)
		if (timestamp.compareTo(period.get(0)) <0 ) return false;
		// else check if ts higher than period's end time (second element in array list)
		if (timestamp.compareTo(period.get(1)) > 0) return false;
		
		// else everything is ok and timestamp belongs inside element's downtime period
		return true;
	}
	
	
	/**
	 * getMetricStatuses receives a StatusNode of type "endpoint" iterates over the
	 * nested children nodes and captures information about all metric nodes included in the group
	 * 
	 * @param egroup
	 *            StatusNode input object of type "endpoint"
	 * @param ops
	 *            OpsManager reference object to translate status ids to string names
	 *            
	 * @return	Map<String,ArrayList<String>> a hashmap of two string arraylists keyed: "metrics", "statuses"
	 *            
	 */
	public Map<String,ArrayList<String>> getMetricStatuses(StatusNode endpoint, OpsManager ops) {
		Map<String, ArrayList<String>> results = new HashMap<String,ArrayList<String>>();
		
		ArrayList<String> metrics = new ArrayList<String>();
		ArrayList<String> statuses = new ArrayList<String>();
		
		results.put("metrics", metrics);
		results.put("statuses", statuses);
		// check if StatusNode is indeed of endpoint group type
		if (endpoint.type.equalsIgnoreCase("endpoint") == false) {
			return results;
		}
		
	
		for (Entry<String, StatusNode> metricEntry : endpoint.children.entrySet()) {
			String metricName = metricEntry.getKey();
			StatusNode metric = metricEntry.getValue();
			// Add endpoint information to results
			results.get("metrics").add(metricName);
			results.get("statuses").add(ops.getStrStatus(metric.item.status));
		}
		
		
		
		return results;
	}

	
	/**
	 * getGroupEndpointStatuses receives a StatusNode of type "endpoint_group" iterates over the
	 * nested children nodes and captures information about all endpoint nodes included in the group
	 * 
	 * @param egroup
	 *            StatusNode input object of type "endpoint group"
	 * @param ops
	 *            OpsManager reference object to translate status ids to string names
	 *            
	 * @return	Map<String,ArrayList<String>> a hashmap of three string arraylists keyed: "endpoints", "services", "statuses"
	 *            
	 */
	public Map<String,ArrayList<String>> getGroupEndpointStatuses(StatusNode egroup, OpsManager ops) {
		Map<String, ArrayList<String>> results = new HashMap<String,ArrayList<String>>();
		ArrayList<String> endpoints = new ArrayList<String>();
		ArrayList<String> services = new ArrayList<String>();
		ArrayList<String> statuses = new ArrayList<String>();
		results.put("endpoints", endpoints);
		results.put("services", services);
		results.put("statuses", statuses);
		// check if StatusNode is indeed of endpoint group type
		if (egroup.type.equalsIgnoreCase("group") == false) {
			return results;
		}
		
		for (Entry<String, StatusNode> serviceEntry : egroup.children.entrySet()) {
			String serviceName = serviceEntry.getKey();
			StatusNode service = serviceEntry.getValue();
			for (Entry<String, StatusNode> endpointEntry : service.children.entrySet()) {
				String endpointName = endpointEntry.getKey();
				StatusNode endpoint = endpointEntry.getValue();
				// Add endpoint information to results
				results.get("endpoints").add(endpointName);
				results.get("services").add(serviceName);
				results.get("statuses").add(ops.getStrStatus(endpoint.item.status));
			}
		}
		
		
		
		return results;
	}

	/**
	 * setStatus accepts an incoming metric event and checks which entities are
	 * affected (changes in status). For each affected entity generates a status
	 * event
	 * 
	 * @param service
	 *            Name of the service in the metric event
	 * @param hostname
	 *            Name of the hostname in the metric event
	 * @param metric
	 *            Name of the metric in the metric event
	 * @param statusStr
	 *            Status value in string format
	 * @param monHost
	 *            Name of the monitoring host that generated the event
	 * @param tsStr
	 *            Timestamp value in string format
	 * @return List of generated events in string json format
	 */
	public ArrayList<String> setStatus(String group, String service, String hostname, String metric, String statusStr, String monHost,
			String tsStr, String summary, String message) throws ParseException {
		ArrayList<String> results = new ArrayList<String>();

		// prepare status events might come up
		StatusEvent evtEgroup = new StatusEvent();
		StatusEvent evtService = new StatusEvent();
		StatusEvent evtEndpoint = new StatusEvent();
		StatusEvent evtMetric = new StatusEvent();
		
		int status = ops.getIntStatus(statusStr);
		Date ts = fromZulu(tsStr);

			
		// Set StatusNodes
		StatusNode groupNode = null;
		StatusNode serviceNode = null;
		StatusNode endpointNode = null;
		StatusNode metricNode = null;
		
		String[] statusMetric = new String[4];
		String[] statusEndpoint = new String[4];
		String[] statusService = new String[4];
		String[] statusEgroup = new String[4];
		

		boolean updMetric = false;
		boolean updEndpoint = false;
		boolean updService = false;
	

		Date oldGroupTS;
		Date oldServiceTS;
		Date oldEndpointTS;
		Date oldMetricTS;
		
		int oldGroupStatus;
		int oldServiceStatus;
		int oldEndpointStatus;
		int oldMetricStatus;

		// Open groups
		groupNode = this.groups.get(group);
		if (groupNode != null) {
			// check if ts is behind groupNode ts
			if (groupNode.item.timestamp.compareTo(ts) > 0)
				return results;
			// update ts
			oldGroupTS = groupNode.item.timestamp;
			oldGroupStatus = groupNode.item.status;
			groupNode.item.timestamp = ts;

			// Open services
			serviceNode = groupNode.children.get(service);

			if (serviceNode != null) {
				// check if ts is behind groupNode ts
				if (serviceNode.item.timestamp.compareTo(ts) > 0)
					return results;
				// update ts
				oldServiceTS = serviceNode.item.timestamp;
				oldServiceStatus = serviceNode.item.status;
				serviceNode.item.timestamp = ts;

				// Open endpoints
				endpointNode = serviceNode.children.get(hostname);

				if (endpointNode != null) {
					// check if ts is behind groupNode ts
					if (endpointNode.item.timestamp.compareTo(ts) > 0)
						return results;
					// update ts
					oldEndpointTS = endpointNode.item.timestamp;
					oldEndpointStatus = endpointNode.item.status;
					endpointNode.item.timestamp = ts;

					// Open metrics
					metricNode = endpointNode.children.get(metric);

					if (metricNode != null) {

						// check if ts is after previous timestamp
						if (metricNode.item.timestamp.compareTo(ts) <= 0) {
							// update status
							boolean repeat = hasTimeDiff(ts,metricNode.item.genTs,this.timeout);
							oldMetricTS = metricNode.item.timestamp;
							oldMetricStatus = metricNode.item.status;
							if (metricNode.item.status != status || repeat ) {
								// generate event
								evtMetric = genEvent("metric", group, service, hostname, metric, ops.getStrStatus(status),
										monHost, ts, ops.getStrStatus(oldMetricStatus), oldMetricTS, repeat, summary, message);
										
								
								// Create metric status level object
								statusMetric = new String[] {evtMetric.getStatus(),evtMetric.getPrevStatus(),evtMetric.getTsMonitored(),evtMetric.getPrevTs()};
								evtMetric.setStatusMetric(statusMetric);
								
								
								results.add(eventToString(evtMetric));
								
								
								
								metricNode.item.status = status;
								metricNode.item.timestamp = ts;
								metricNode.item.genTs = ts;
								updMetric = true;
							}

							
						}

					}
					// If metric indeed updated -> aggregate endpoint
					if (updMetric) {
						// calculate endpoint new status
						int endpNewStatus = aggregate("", endpointNode, ts);
						// check if status changed
						boolean repeat = hasTimeDiff(ts,endpointNode.item.genTs,this.timeout);
						

						// generate event
						evtEndpoint = genEvent("endpoint", group, service, hostname, metric,
								ops.getStrStatus(endpNewStatus), monHost, ts,
								ops.getStrStatus(oldEndpointStatus), oldEndpointTS,repeat,summary,message);
						
						// Create metric,endpoint status level object
						statusEndpoint = new String[] {evtEndpoint.getStatus(),evtEndpoint.getPrevStatus(), evtEndpoint.getTsMonitored(), evtEndpoint.getPrevTs()};
						
						evtEndpoint.setStatusMetric(statusMetric);
						evtEndpoint.setStatusEndpoint(statusEndpoint);
						
						// generate group endpoint information 
						Map<String, ArrayList<String>> metricStatuses = getMetricStatuses(endpointNode,ops);
						evtEndpoint.setMetricNames( metricStatuses.get("metrics").toArray(new String[0]));
						evtEndpoint.setMetricStatuses( metricStatuses.get("statuses").toArray(new String[0]));
						
						results.add(eventToString(evtEndpoint));
						
						
						
						endpointNode.item.status = endpNewStatus;
						endpointNode.item.genTs = ts;
						updEndpoint = true;
						

					}
				}
				// if endpoint indeed updated -> aggregate service
				if (updEndpoint) {
					// calculate service new status
					int servNewStatus = aggregate(service, serviceNode, ts);
					// check if status changed
					boolean repeat = hasTimeDiff(ts,groupNode.item.genTs,this.timeout);
					

					// generate event
					evtService = genEvent("service", group, service, hostname, metric, ops.getStrStatus(servNewStatus),
							monHost, ts, ops.getStrStatus(oldServiceStatus), oldServiceTS,repeat,summary,message);
					
					
					// Create metric, endpoint, service status metric objects
					statusService = new String[] {evtService.getStatus(),evtService.getPrevStatus(), evtService.getTsMonitored(), evtService.getPrevTs()};
					
					evtService.setStatusMetric(statusMetric);
					evtService.setStatusEndpoint(statusEndpoint);
					evtService.setStatusService(statusService);
					
					
					results.add(eventToString(evtService));
					serviceNode.item.status = servNewStatus;
					serviceNode.item.genTs=ts;
					updService = true;

					

				}
			}
			// if service indeed updated -> aggregate group
			if (updService) {
				// calculate group new status
				int groupNewStatus = aggregate(group, groupNode, ts);
				// check if status changed
				boolean repeat = hasTimeDiff(ts,groupNode.item.genTs,this.timeout);
				
					
				// generate event
				
				evtEgroup = genEvent("endpoint_group", group, service, hostname, metric, ops.getStrStatus(groupNewStatus),
						monHost, ts, ops.getStrStatus(oldGroupStatus), oldGroupTS,repeat,summary,message);
				
				// Create metric, endpoint, service, egroup status metric objects
				statusEgroup = new String[] {evtEgroup.getStatus(),evtEgroup.getPrevStatus(), evtEgroup.getTsMonitored(), evtEgroup.getPrevTs()};
				
				// generate group endpoint information 
				Map<String, ArrayList<String>> groupStatuses = getGroupEndpointStatuses(groupNode,ops);
				evtEgroup.setGroupEndpoints( groupStatuses.get("endpoints").toArray(new String[0]));
				evtEgroup.setGroupServices( groupStatuses.get("services").toArray(new String[0]));
				evtEgroup.setGroupStatuses( groupStatuses.get("statuses").toArray(new String[0]));
				
				
				evtEgroup.setStatusMetric(statusMetric);
				evtEgroup.setStatusEndpoint(statusEndpoint);
				evtEgroup.setStatusService(statusService);
				evtEgroup.setStatusEgroup(statusEgroup);
				
				results.add(eventToString(evtEgroup));
				
				groupNode.item.status = groupNewStatus;
				groupNode.item.genTs = ts;
					
				
			}
		}
		// If service host combination has downtime clear result set
		if (hasDowntime(tsStr,hostname,service)){
			LOG.info("Downtime encountered for group:{},service:{},host:{} - events will be discarded",group,service,hostname);
			results.clear();
		}
		
		
		return results;
		
	}


	/**
	 * Generates a status event
	 * 
	 * @param type
	 *            Name of event type
	 * @param group
	 *            Name of the endpoint group in the metric event
	 * @param service
	 *            Name of the service in the metric event
	 * @param hostname
	 *            Name of the hostname in the metric event
	 * @param metric
	 *            Name of the metric in the metric event
	 * @param statusStr
	 *            Status value in string format
	 * @param monHost
	 *            Name of the monitoring host that affected the event
	 * @param tsStr
	 *            Timestamp value in string format
	 * @return A string containing the event in json format
	 */
	private StatusEvent genEvent(String type, String group, String service, String hostname, String metric, String status,
			String monHost, Date ts, String prevStatus, Date prevTs, boolean repeat, String summary, String message) throws ParseException {
		String tsStr = toZulu(ts);
		String dt = tsStr.split("T")[0].replaceAll("-", "");
		String tsProc = toZulu(new Date());
		
		if (summary==null) {
			summary="";
		}
		if (message==null) {
			message="";
		}
		
		StatusEvent evnt = new StatusEvent(this.report, type, dt, group, service, hostname, metric, status, monHost,
				toZulu(ts), tsProc, prevStatus, toZulu(prevTs), new Boolean(repeat).toString(),summary, message );
		

		return evnt;
	}
	
	/**
	 * Accepts a StatusEvent object and returns a json string representation of it
	 * 
	 * @param evnt
	 * @return A json string representation of a Status Event
	 */
	private String eventToString(StatusEvent evnt) {
		Gson gson = new Gson();
		String evntJson = gson.toJson(evnt);
		LOG.debug("Event Generated: " + evntJson);
		return evntJson;
	}

	/**
	 * Aggregate status values according to profiles
	 * 
	 * @param node
	 *            Status node used to aggregate its children
	 * @param ts
	 *            Timestamp of the aggregation event
	 * @return Status value in integer format
	 */
	public int aggregate(String itemName, StatusNode node, Date ts) {

		// get aggregation profile used (1st one in the list)
		String aggProfile = aps.getAvProfiles().get(0);
		
		
		
	

		// Iterate on children nodes
		Iterator<Entry<String, StatusNode>> valIter = node.children.entrySet().iterator();
		Entry<String, StatusNode> item = valIter.next();
		StatusNode a = item.getValue();
		StatusNode b = null;
		int res = a.item.status;

		
		if (node.type.equals("group")) {

			// Create a hashmap for the aggregation groups
			Map<String, Integer> aGroups = new HashMap<String, Integer>();
			// If aggregation target is group then each hashmap item key is the service name
			String serviceName = item.getKey();
			String groupName = aps.getGroupByService(aggProfile, serviceName);
			// aggregation hashmap is empty so insert the first item
			aGroups.put(groupName, a.item.status);
			// Iterate over rest of the service items
			while (valIter.hasNext()) {
				// next item in iteration
				item = valIter.next();
				// get the service name from key
				serviceName = item.getKey();
				// get the item status information
				b = item.getValue();
				// get the aggregation group name based on service name
				groupName = aps.getGroupByService(aggProfile, serviceName);
				// Now that aggregation hashmap is surely not empty check if groupname exists
				if (aGroups.containsKey(groupName)) {
					// aggregate the existing value with the new one
					// get the appropriate aggregation operation for this service group
					int gOp = ops.getIntOperation(aps.getProfileGroupServiceOp(aggProfile, groupName, serviceName));
					// get the existing value from the hashmap
					res = aGroups.get(groupName).intValue();
					// calculate the new value
					res = ops.opInt(gOp, res, b.item.status);
					aGroups.put(groupName, res);

				} else { // if groupname doesn't exist add it 
					aGroups.put(groupName, b.item.status);
				}
			}

			// after completing the individual group aggregations aggregate the total value
			int totalOp = ops.getIntOperation(aps.getTotalOp(aggProfile));
			// iterate over the group aggregations
			Iterator<Entry<String, Integer>> aggIter = aGroups.entrySet().iterator();
			res = aggIter.next().getValue();
			// second value to be aggregated in each iteration
			int bItem;
			while (aggIter.hasNext()) {
				bItem = aggIter.next().getValue();
				res = ops.opInt(totalOp, res, bItem);
			}

		} else {
			

			
			
			
			// aggregate according to rest of the types
			while (valIter.hasNext()) {
				b = valIter.next().getValue();
				if (node.type.equals("endpoint")) {
					int mOp = ops.getIntOperation(aps.getMetricOp(aggProfile));
					res = ops.opInt(mOp, res, b.item.status);
				} else if (node.type.equals("service")) {
					
					String groupName = aps.getGroupByService(aggProfile, itemName);
					int eOp = ops.getIntOperation(aps.getProfileGroupServiceOp(aggProfile, groupName, itemName));
					res = ops.opInt(eOp, res, b.item.status);
				} else if (node.type.equals("group")) {
					//res = ops.opInt(sOp, res, b.item.status);
				}
			}
		}

		return res;
	}

}