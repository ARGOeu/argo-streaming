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
import com.google.gson.annotations.SerializedName;


public class StatusManager {

	static Logger LOG = LoggerFactory.getLogger(StatusManager.class);
	
	EndpointGroups egp = new EndpointGroups();
	MetricProfiles mps = new MetricProfiles();
	AvailabilityProfiles aps = new AvailabilityProfiles();
	OpsManager ops = new OpsManager();
	
	String validMetricProfile;
	String validAggProfile;
	ArrayList<String> validServices = new ArrayList<String>();
	Map<String,StatusNode> groups = new HashMap<String,StatusNode>();
	
	public OpsManager getOps(){
		return this.ops;
	}
	
	public class StatusItem{
		int status;
		Date timestamp;
	}
	
	public class StatusNode{
		String type;
		StatusItem item;
		Map<String,StatusNode> children = new HashMap<String,StatusNode>();
		StatusNode parent = null;
		
		public StatusNode(String type, int defStatus, Date defTs){
			this.type = type;
			this.item = new StatusItem();
			this.item.status=defStatus;
			this.item.timestamp = defTs;
			this.parent = null;	
		}
		
		public StatusNode(String type, int defStatus, Date defTs, StatusNode parent)
		{
			this.type= type;
			this.item = new StatusItem();
			this.item.status=defStatus;
			this.item.timestamp = defTs;
			this.parent = parent;
		}
		
	}
	
	
	
	public Date getToday(){
		Calendar cal = Calendar.getInstance();
	    cal.set(Calendar.HOUR_OF_DAY, 0);
	    cal.set(Calendar.MINUTE, 0);
	    cal.set(Calendar.SECOND, 0);
	    return cal.getTime();
	}
	
	public Date setDate(String zulu) throws ParseException{
		String[] parts = zulu.split("T");
		return fromZulu(parts[0]+"T00:00:00Z");
	}
	
	public void setValidProfileServices(){
		//Get services from first profile
		this.validMetricProfile = this.mps.getProfiles().get(0);
		this.validAggProfile = this.aps.getAvProfiles().get(0);
		this.validServices = this.mps.getProfileServices(this.validMetricProfile);
	}
	
	public void loadAll(File egpAvro, File mpsAvro, File apsJson, File opsJson ) throws IOException{
		egp.loadAvro(egpAvro);
		mps.loadAvro(mpsAvro);
		aps.loadJson(apsJson);
		ops.loadJson(opsJson);
		
		setValidProfileServices();
	}
	
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
	
	public void addMetrics(StatusNode endpointNode,String service, String hostname,int defStatus, Date defTs){
		ArrayList<String> metrics = this.mps.getProfileServiceMetrics(this.validMetricProfile, service);
		// For all available metrics create leaf metric nodes
		for (String metric : metrics){
			StatusNode metricNode = new StatusNode("metric",defStatus,defTs,endpointNode);
			metricNode.children=null;
			endpointNode.children.put(metric, metricNode);
		}
	}
	
	public Date fromZulu(String zulu) throws ParseException{
		DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		Date date = utcFormat.parse(zulu);
		return date;
	}
	
	public String toZulu (Date ts) throws ParseException{
		DateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		return utcFormat.format(ts);
	}
	
	public ArrayList<String> setStatus(String service, String hostname, String metric, String statusStr, String tsStr) throws ParseException{
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
		// Open groups
		groupNode = this.groups.get(group);
		if (groupNode != null){
			// check if ts is behind groupNode ts
			if (groupNode.item.timestamp.compareTo(ts) > 0) return results;
			// update ts
			groupNode.item.timestamp = ts;
			
			
			// Open services
			serviceNode = groupNode.children.get(service);
			
			if (serviceNode != null){
				// check if ts is behind groupNode ts
				if (serviceNode.item.timestamp.compareTo(ts) > 0) return results;
				// update ts
				serviceNode.item.timestamp = ts;
				
				// Open endpoints
				endpointNode = serviceNode.children.get(hostname);
				
				if (endpointNode != null){
					// check if ts is behind groupNode ts
					if (endpointNode.item.timestamp.compareTo(ts) > 0) return results;
					// update ts
					endpointNode.item.timestamp = ts;
					
					// Open metrics
					metricNode = endpointNode.children.get(metric);
					
					if (metricNode != null){
						
						// check if ts is after previous timestamp
						if (endpointNode.item.timestamp.compareTo(ts) <= 0) {
							// update status
							metricNode.item.status = status;
							metricNode.item.timestamp = ts;
							updMetric = true;
							// generate event
							results.add(genEvent("metric",group,service,hostname,metric,ops.getStrStatus(status),ts));
						}
						
					}
					// If metric indeed updated -> aggregate endpoint
					if (updMetric)
					{
						// calculate endpoint new status
						int endpNewStatus = aggregate(endpointNode,ts);
						// check if status changed
						if (endpointNode.item.status != endpNewStatus){
							endpointNode.item.status = endpNewStatus;
							updEndpoint = true;
							// generate event
							results.add(genEvent("endpoint",group,service,hostname,"",ops.getStrStatus(endpNewStatus),ts));
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
						serviceNode.item.status = servNewStatus;
						updService = true;
						// generate event
						results.add(genEvent("service",group,service,"","",ops.getStrStatus(servNewStatus),ts));
						
						
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
					groupNode.item.status = groupNewStatus;
					updGroup = true;
					// generate event
					results.add(genEvent("endpoint_group",group,"","","",ops.getStrStatus(groupNewStatus),ts));
				}
			}
		}
		
		return results;
	}
	
	private String genEvent(String type,String group, String service, String host, String metric, String status, Date ts) throws ParseException{
		String tsProc = toZulu(new Date());
		StatusEvent evnt = new StatusEvent(type,group,service,host,metric,status,toZulu(ts),tsProc);
	    Gson gson = new Gson();
		return gson.toJson(evnt);
	}
	
	public int aggregate(StatusNode node,Date ts)
	{
		int mOp = ops.getIntOperation("AND");
		int eOp = ops.getIntOperation("OR");
		int sOp = ops.getIntOperation("AND");
		
		Iterator<Entry<String, StatusNode>> valIter = node.children.entrySet().iterator();
		StatusNode a = valIter.next().getValue();
		StatusNode b = null;
		int res=a.item.status;
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
