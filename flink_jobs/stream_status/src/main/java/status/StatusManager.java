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

import java.util.Set;
import java.util.TimeZone;

import sync.*;
import sync.EndpointGroups.EndpointItem;
import ops.OpsManager;

import com.google.gson.Gson;


/**
 *  Status Manager implements a live structure containing a topology of entities
 *  and the related statuses for each one 
 */
public class StatusManager {

	// Initialize logger
	static Logger LOG = LoggerFactory.getLogger(StatusManager.class);
	
	// Name of the report used
	String report;
	
	// Sync file structures necessary for status computation
	EndpointGroups egp = new EndpointGroups();
	MetricProfiles mps = new MetricProfiles();
	AvailabilityProfiles aps = new AvailabilityProfiles();
	OpsManager ops = new OpsManager();
	
	// Names of valid profiles and services used
	String validMetricProfile;
	String validAggProfile;
	ArrayList<String> validServices = new ArrayList<String>();
	
	// Structure to hold topology entities and related statuses
	Map<String,StatusNode> groups = new HashMap<String,StatusNode>();
	
	// Flag used in initial event generation
	Boolean firstGen = true;
	
	// Timestamp of the latest processed event used as a daily event generation trigger
	String tsLatest;

	// Get Operation Manager 
	public OpsManager getOps(){
		return this.ops;
	}
	
	
	/**
	 *  Status Item is a simple structure holding the latest status
	 *  for an entity along with the timestamp of the event
	 */
	public class StatusItem{
		int status;
		Date timestamp;
	}
	
	/**
	 *  Status Node represents status information for entity in the topology tree
	 *  An entity might contain other entities with status information
	 */
	public class StatusNode{
		// Type of entity: endpoint_group,service,endpoint or metric
		String type;
		// Status information with timestamp
		StatusItem item;
		// A list of entities contained as children
		Map<String,StatusNode> children = new HashMap<String,StatusNode>();
		// Reference to the parent node
		StatusNode parent = null;
		
		/**
		 *  Creates a new status node
		 *  @param type         A string containing the node type (endpoint_group,service,endpoint,metric)
		 *  @param defStatus    Default status value
		 *  @param defTs        Default timestamp
		 */
		public StatusNode(String type, int defStatus, Date defTs){
			this.type = type;
			this.item = new StatusItem();
			this.item.status=defStatus;
			this.item.timestamp = defTs;
			this.parent = null;	
		}
		
		/**
		 *  Creates a new status node given a parent
		 *  @param type         A string containing the node type (endpoint_group,service,endpoint,metric)
		 *  @param defStatus    Default status value
		 *  @param defTs        Default timestamp
		 *  @param parent       Reference to the parent status node
		 */
		public StatusNode(String type, int defStatus, Date defTs, StatusNode parent)
		{
			this.type= type;
			this.item = new StatusItem();
			this.item.status=defStatus;
			this.item.timestamp = defTs;
			this.parent = parent;
		}
		
	}
	
	/**
	 *  Set the latest processed timestamp value
	 */
	public void setTsLatest(String ts){
		this.tsLatest = ts;
	}
	
	/**
	 *  Get the latest processed timestamp value
	 */
	public String getTsLatest(){
		return this.tsLatest;
	}
	
	/**
	 *  Disable flag for initial event generation
	 */
	public void disableFirstGen(){
		this.firstGen = false;
	}
	
	/**
	 *  Check if day has changed between two sequential timestamps
	 * 
	 *  @param tsOld Previous timestamp
	 *  @param tsNew Newest timestamp
	 */
	public boolean hasDayChanged(String tsOld, String tsNew){
		String dtOld = tsOld.split("T")[0];
		String dtNew= tsNew.split("T")[0];
		
		if (dtOld.compareToIgnoreCase(dtNew) != 0){
			return true;
		}
		
		return false;
	}
	
	/**
	 *  Get firstGen parameter flag to check if initial event generation is needed
	 */
	public boolean getFirstGen(){
		return this.firstGen;
	}
	
	/**
	 *  Get today's datetime at the beginning of day
	 *  @return Date at the beginning of day
	 */
	public Date getToday(){
		Calendar cal = Calendar.getInstance();
	    cal.set(Calendar.HOUR_OF_DAY, 0);
	    cal.set(Calendar.MINUTE, 0);
	    cal.set(Calendar.SECOND, 0);
	    return cal.getTime();
	}
	
	/**
	 *  Convert zulu timestamp in date object
	 *  @param  zulu String representing a zulu timestamp
	 *  @return 	 Date object
	 */
	public Date setDate(String zulu) throws ParseException{
		String[] parts = zulu.split("T");
		return fromZulu(parts[0]+"T00:00:00Z");
	}
	
	/**
	 *  Compare profiles for validity and extract valid services
	 */
	public void setValidProfileServices(){
		//Get services from first profile
		this.validMetricProfile = this.mps.getProfiles().get(0);
		this.validAggProfile = this.aps.getAvProfiles().get(0);
		this.validServices = this.mps.getProfileServices(this.validMetricProfile);
	}
	
	/**
	 *  Load all initial Profiles
	 *    
	 *  @param egpAvro  endpoint group topology location
	 *  @param mpsAvro  metric profile location
	 *  @param apsJson  aggregation profile location
	 *  @param opsJson  operation profile location
	 */
	public void loadAll(File egpAvro, File mpsAvro, File apsJson, File opsJson ) throws IOException{
		egp.loadAvro(egpAvro);
		mps.loadAvro(mpsAvro);
		aps.loadJson(apsJson);
		ops.loadJson(opsJson);
		
		setValidProfileServices();
	}
	
	/**
	 *  Construct status topology with initial status value and timestamp
	 *  
	 *  @param defStatus    Initial status to be used
	 *  @param defTs        Initial timestamp to be used
	 */
	public void construct(int defStatus, Date defTs){
		// Get all the available hosts
		Iterator<EndpointItem> hostIter = egp.getIterator();
		// for each host entry iterate
		while (hostIter.hasNext()){
			EndpointItem host = hostIter.next();
			String service = host.getService();
			String group = host.getGroup();
			String hostname = host.getHostname();
			
			if (this.validServices.contains(service)){
				//Add host to groups
				addGroup(group,service,hostname,defStatus,defTs);
			}
			
		}
	}
	
	/**
	 *  Add a new endpoint group node to the status topology using metric data information
	 *  
	 *  @param  group       Name of the endpoint group
	 *  @param  service     Name of the service flavor
	 *  @param  hostname    Name of the endpoint
	 *  @param  defStatus   Default status to be initialized to
	 *  @param  defTs       Default timestamp to be initialized to
	 */
	public void addGroup(String group, String service, String hostname,int defStatus, Date defTs){
		// Check if group exists
		if (!this.groups.containsKey(group)){
			StatusNode groupNode = new StatusNode("group",defStatus,defTs);
			this.groups.put(group, groupNode);
			// Add to the new node
			addService(groupNode,service,hostname,defStatus,defTs);
			return;
		}
		
		// Find group node and continue adding service under there
		addService(this.groups.get(group),service,hostname,defStatus,defTs);
		
	}
	
	/**
	 *  Add a new service node to the status topology using metric data information
	 *  
	 *  @param  groupNode   Reference to the parent node
	 *  @param  service     Name of the service flavor
	 *  @param  hostname    Name of the endpoint
	 *  @param  defStatus   Default status to be initialized to
	 *  @param  defTs       Default timestamp to be initialized to
	 */
	public void addService(StatusNode groupNode,String service,String hostname,int defStatus, Date defTs){
		if (!groupNode.children.containsKey(service)){
			StatusNode serviceNode = new StatusNode("service",defStatus,defTs,groupNode);
			groupNode.children.put(service, serviceNode);
			// Add to the new node
			addEndpoint(serviceNode,service,hostname,defStatus,defTs);
			return;
		}
		
		// Find service node and continue adding endpoint under there
		addEndpoint(groupNode.children.get(service),service,hostname,defStatus,defTs);
	}
	
	/**
	 *  Add a new endpoint node to the status topology using metric data information
	 *  
	 *  @param  serviceNode Reference to the parent node
	 *  @param  service     Name of the service flavor
	 *  @param  hostname    Name of the endpoint
	 *  @param  defStatus   Default status to be initialized to
	 *  @param  defTs       Default timestamp to be initialized to
	 */
	public void addEndpoint(StatusNode serviceNode,String service, String hostname,int defStatus, Date defTs){
		if (!serviceNode.children.containsKey(hostname)){
			StatusNode endpointNode = new StatusNode("endpoint",defStatus,defTs,serviceNode);
			serviceNode.children.put(hostname, endpointNode);
			// Add to the new node
			addMetrics(endpointNode,service,hostname,defStatus,defTs);
			return;
		}
		
		// Find endpoint node and continue adding metrics under there
		addMetrics(serviceNode.children.get(hostname),service,hostname,defStatus,defTs);
	}
	
	/**
	 *  Add a new metrics node to the status topology using metric data information
	 *  
	 *  @param  endpointNode    Reference to the parent node
	 *  @param  service         Name of the service flavor
	 *  @param  hostname        Name of the endpoint
	 *  @param  defStatus       Default status to be initialized to
	 *  @param  defTs           Default timestamp to be initialized to
	 */
	public void addMetrics(StatusNode endpointNode,String service, String hostname,int defStatus, Date defTs){
		ArrayList<String> metrics = this.mps.getProfileServiceMetrics(this.validMetricProfile, service);
		// For all available metrics create leaf metric nodes
		for (String metric : metrics){
			StatusNode metricNode = new StatusNode("metric",defStatus,defTs,endpointNode);
			metricNode.children=null;
			endpointNode.children.put(metric, metricNode);
		}
	}
	
	/**
	 *  Convert a timestamp string to date object
	 *  
	 *  @param  zulu String with timestamp in zulu format
	 *  @return      Date object
	 */
	public Date fromZulu(String zulu) throws ParseException{
		DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = utcFormat.parse(zulu);
		return date;
	}
	
	/**
	 *  Convert a date object to a string timestamp in zulu format
	 *  
	 *  @param  ts  Date object to be converted
	 *  @return     String with timestamp in zulu format
	 */
	public String toZulu (Date ts) throws ParseException{
		DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return utcFormat.format(ts);
	}
	
	/**
	 *  For all entities in the topology generate status events
	 *  
	 *  @param  tsStr   String containing timestamp of status generation
	 *  @return         List of generated events in string json format
	 */
	public ArrayList<String> dumpStatus(String tsStr) throws ParseException{
		// Convert timestamp to date object
		Date ts = fromZulu(tsStr);
		// Initialize event list
		ArrayList<String> results = new ArrayList<String>();
		
		// For each endpoint group in topology
		for (String groupName : groups.keySet()){
			StatusNode groupNode = groups.get(groupName);
			String groupStatus = ops.getStrStatus(groupNode.item.status);
			Date groupTs = groupNode.item.timestamp;
			
			// For each service in the specific endpoint group
			for (String serviceName : groupNode.children.keySet()){
				StatusNode serviceNode = groupNode.children.get(serviceName);
				String serviceStatus = ops.getStrStatus(serviceNode.item.status);
				Date serviceTs = serviceNode.item.timestamp;
				
				// For each endpoint in the specific service
				for (String endpointName : serviceNode.children.keySet()){
					StatusNode endpointNode = serviceNode.children.get(endpointName);
					String endpointStatus = ops.getStrStatus(endpointNode.item.status);
					Date endpointTs = endpointNode.item.timestamp;
					
					// For each metric in the specific service endpoint
					for (String metricName: endpointNode.children.keySet()){
						StatusNode metricNode = endpointNode.children.get(metricName);
						String metricStatus = ops.getStrStatus(metricNode.item.status);
						Date metricTs = metricNode.item.timestamp;
						// Generate metric status event
						results.add(genEvent("metric",groupName,serviceName,endpointName,metricName,metricStatus,"",metricTs,metricStatus,metricTs));
					}
					// Generate endpoint status event
					results.add(genEvent("endpoint",groupName,serviceName,endpointName,"",endpointStatus,"",ts,endpointStatus,endpointTs));
				}
				// Generate service status event
				results.add(genEvent("service",groupName,serviceName,"","",serviceStatus,"",ts,serviceStatus,serviceTs));
			}
			// Generate endpoint group status event
			results.add(genEvent("grpoup",groupName,"","","",groupStatus,"",ts,groupStatus,groupTs));
		}
		
		
		return results;
	}
	
	/**
	 *  setStatus accepts an incoming metric event and checks which entities are affected (changes in status).
	 *  For each affected entity generates a status event
	 *  
	 *  @param  service     Name of the service in the metric event
	 *  @param  hostname    Name of the hostname in the metric event
	 *  @param  metric      Name of the metric in the metric event
	 *  @param  statusStr   Status value in string format
	 *  @param  monHost     Name of the monitoring host that generated the event
	 *  @param  tsStr       Timestamp value in string format	 
	 *  @return             List of generated events in string json format
	 */
	public ArrayList<String> setStatus(String service, String hostname, String metric, String statusStr, String monHost, String tsStr) throws ParseException{
		ArrayList<String> results = new ArrayList<String>();
		
		int status = ops.getIntStatus(statusStr);
		Date ts = fromZulu(tsStr);
		
		// Get group from hostname,service
		ArrayList<String> groups = egp.getGroup("SITES", hostname, service);
		if (groups.size()==0) return results;
		String group = groups.get(0);
		LOG.info(group);
		// Set StatusNodes
		StatusNode groupNode = null;
		StatusNode serviceNode = null;
		StatusNode endpointNode = null;
		StatusNode metricNode = null;
		
		boolean updMetric = false;
		boolean updEndpoint = false;
		boolean updService = false;
		boolean updGroup = false;
		
		Date oldGroup;
		Date oldService;
		Date oldEndpoint;
		Date oldMetric;
		
		// Open groups
		groupNode = this.groups.get(group);
		if (groupNode != null){
			// check if ts is behind groupNode ts
			if (groupNode.item.timestamp.compareTo(ts) > 0) return results;
			// update ts
			oldGroup = groupNode.item.timestamp;
			groupNode.item.timestamp = ts;
			
			
			
			// Open services
			serviceNode = groupNode.children.get(service);
			
			if (serviceNode != null){
				// check if ts is behind groupNode ts
				if (serviceNode.item.timestamp.compareTo(ts) > 0) return results;
				// update ts
				oldService = serviceNode.item.timestamp;
				serviceNode.item.timestamp = ts;
				
				// Open endpoints
				endpointNode = serviceNode.children.get(hostname);
				
				if (endpointNode != null){
					// check if ts is behind groupNode ts
					if (endpointNode.item.timestamp.compareTo(ts) > 0) return results;
					// update ts
					oldEndpoint = endpointNode.item.timestamp;
					endpointNode.item.timestamp = ts;
					
					// Open metrics
					metricNode = endpointNode.children.get(metric);
					
					if (metricNode != null){
						
						// check if ts is after previous timestamp
						if (endpointNode.item.timestamp.compareTo(ts) <= 0) {
							// update status
							oldMetric = metricNode.item.timestamp;
							// generate event
							results.add(genEvent("metric",group,service,hostname,metric,ops.getStrStatus(status),monHost,ts,ops.getStrStatus(metricNode.item.status),oldMetric));
							metricNode.item.status = status;
							metricNode.item.timestamp = ts;
							
							updMetric = true;
						}
						
						
					}
					// If metric indeed updated -> aggregate endpoint
					if (updMetric)
					{
						// calculate endpoint new status
						int endpNewStatus = aggregate(endpointNode,ts);
						// check if status changed
						if (endpointNode.item.status != endpNewStatus){
							
							// generate event
							results.add(genEvent("endpoint",group,service,hostname,"",ops.getStrStatus(endpNewStatus),monHost,ts,ops.getStrStatus(endpointNode.item.status),oldEndpoint));
							endpointNode.item.status = endpNewStatus;
							updEndpoint = true;
						}
						
					}
				}
				// if endpoint indeed updated -> aggregate service
				if (updEndpoint)
				{
					// calculate service new status
					int servNewStatus = aggregate(serviceNode,ts);
					// check if status changed
					if (serviceNode.item.status != servNewStatus){
						
						// generate event
						results.add(genEvent("service",group,service,"","",ops.getStrStatus(servNewStatus),monHost,ts,ops.getStrStatus(serviceNode.item.status),oldService));
						serviceNode.item.status = servNewStatus;
						updService = true;
						
					}
					
				}
			}
			// if service indeed updated -> aggregate group
			if (updService)
			{
				// calculate group new status
				int groupNewStatus = aggregate(groupNode,ts);
				// check if status changed
				if (groupNode.item.status != groupNewStatus){
					
					// generate event
					results.add(genEvent("endpoint_group",group,"","","",ops.getStrStatus(groupNewStatus),monHost,ts,ops.getStrStatus(groupNode.item.status),oldGroup));
					groupNode.item.status = groupNewStatus;
					updGroup = true;
				}
			}
		}
		
		return results;
	}
	
	
	
	/**
	 *  Generates a status event
	 *  
	 *  @param  type        Name of event type
	 *  @param  group       Name of the endpoint group in the metric event
	 *  @param  service     Name of the service in the metric event
	 *  @param  hostname    Name of the hostname in the metric event
	 *  @param  metric      Name of the metric in the metric event
	 *  @param  statusStr   Status value in string format
	 *  @param  monHost     Name of the monitoring host that affected the event
	 *  @param  tsStr       Timestamp value in string format	 
	 *  @return             A string containing the event in json format
	 */
	private String genEvent(String type,String group, String service, String hostname,  String metric, String status, String monHost, Date ts, String prevStatus, Date prevTs) throws ParseException{
		String tsStr = toZulu(ts);
		String dt = tsStr.split("T")[0].replaceAll("-", "");
		String tsProc = toZulu(new Date());
		StatusEvent evnt = new StatusEvent(this.report,type,dt,group,service,hostname,metric,status,monHost,toZulu(ts),tsProc,prevStatus,toZulu(prevTs));
	    Gson gson = new Gson();
		return gson.toJson(evnt);
	}
	
	/**
	 *  Aggregate status values according to profiles
	 *  
	 *  @param  node        Status node used to aggregate its children 
	 *  @param  ts          Timestamp of the aggregation event 
	 *  @return             Status value in integer format
	 */
	public int aggregate(StatusNode node,Date ts)
	{
		// Establish operations
		int mOp = ops.getIntOperation("AND");
		int eOp = ops.getIntOperation("OR");
		int sOp = ops.getIntOperation("AND");
		
		// Iterate on children nodes
		Iterator<Entry<String, StatusNode>> valIter = node.children.entrySet().iterator();
		StatusNode a = valIter.next().getValue();
		StatusNode b = null;
		int res=a.item.status;
		// Calculate according to type
		while (valIter.hasNext()){
			b = valIter.next().getValue();
			if (node.type.equals("endpoint")){
				res = ops.opInt(mOp, res, b.item.status);
			} else if (node.type.equals("service")){
				res = ops.opInt(eOp, res, b.item.status);
			} else if (node.type.equals("group")){
				res = ops.opInt(sOp, res, b.item.status);
			}
			
		}
		
		return res;
	}
	
	
}
