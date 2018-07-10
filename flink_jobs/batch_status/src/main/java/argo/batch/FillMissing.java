package argo.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import ops.ConfigManager;
import ops.OpsManager;
import sync.AggregationProfileManager;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;

/**
 * Accepts a list of metric data objects and produces a list of missing mon data objects
 */
public class FillMissing extends RichGroupReduceFunction<MetricData, StatusMetric> {

	private static final long serialVersionUID = 1L;

	final ParameterTool params;

	public FillMissing(ParameterTool params) {
		this.params = params;
	}

	static Logger LOG = LoggerFactory.getLogger(ArgoStatusBatch.class);

	private List<MetricProfile> mps;
	private List<String> ops;
	private List<GroupEndpoint> egp;
	private List<GroupGroup> ggp;
	private List<String> conf;
	private MetricProfileManager mpsMgr;
	private EndpointGroupManager egpMgr;
	private GroupGroupManager ggpMgr;
	private OpsManager opsMgr;
	private ConfigManager confMgr;
	private String runDate;
	private String egroupType;
	private Set<Tuple4<String, String, String, String>> expected;

	/**
	 * Initialization method of the RichGroupReduceFunction operator
	 * <p>
	 * This runs at the initialization of the operator and receives a
	 * configuration parameter object. It initializes all required structures
	 * used by this operator such as profile managers, operations managers,
	 * topology managers etc.
	 *
	 * @param parameters
	 *            A flink Configuration object
	 */
	@Override
	public void open(Configuration parameters) throws IOException {

		this.runDate = params.getRequired("run.date");
		// Get data from broadcast variables
		this.mps = getRuntimeContext().getBroadcastVariable("mps");
		this.ops = getRuntimeContext().getBroadcastVariable("ops");
		this.egp = getRuntimeContext().getBroadcastVariable("egp");
		this.ggp = getRuntimeContext().getBroadcastVariable("ggp");
		this.conf = getRuntimeContext().getBroadcastVariable("conf");
		
		// Initialize metric profile manager
		this.mpsMgr = new MetricProfileManager();
		this.mpsMgr.loadFromList(mps);
		// Initialize operations manager
		this.opsMgr = new OpsManager();
		this.opsMgr.loadJsonString(ops);

		// Initialize endpoint group manager
		this.egpMgr = new EndpointGroupManager();
		this.egpMgr.loadFromList(egp);

		this.ggpMgr = new GroupGroupManager();
		this.ggpMgr.loadFromList(ggp);
		
		this.confMgr = new ConfigManager();
		this.confMgr.loadJsonString(conf);

		this.runDate = params.getRequired("run.date");
		this.egroupType = this.confMgr.egroup;
		
		

	}

	
	/**
	 * Reads the topology in endpoint group list and the metric profile and produces a set of available service endpoint metrics
	 * that are expected to be found (as tuple objects (endpoint_group,service,hostname,metric)
	 **/
	public void initExpected() {
		this.expected = new HashSet<Tuple4<String, String, String, String>>();
		String mProfile = this.mpsMgr.getProfiles().get(0);
		for (GroupEndpoint servPoint: this.egp){
			
			
			ArrayList<String> metrics = this.mpsMgr.getProfileServiceMetrics(mProfile, servPoint.getService());
			
			if (metrics==null) continue;
			for (String metric:metrics){
				this.expected.add(new Tuple4<String,String,String,String>(servPoint.getGroup(),servPoint.getService(),servPoint.getHostname(),metric));
			}
			
		}
		
	
		
		

	}

	/**
	 * Iterates over all metric data and gathers a set of encountered service endpoint metrics. Then subtracts it from 
	 * a set of expected service endpoint metrics (based on topology) so as the missing service endpoint metrics to be identified. Then based on the 
	 * list of the missing service endpoint metrics corresponding metric data are created
	 *
	 * @param in
	 *            An Iterable collection of MetricData objects
	 * @param out
	 *            A Collector list of Missing MonData objects
	 */
	@Override
	public void reduce(Iterable<MetricData> in, Collector<StatusMetric> out) throws Exception {
		
		initExpected();

		Set<Tuple4<String, String, String, String>> found = new HashSet<Tuple4<String, String, String, String>>();

		String service = "";
		String endpointGroup = "";
		String hostname = "";
		String metric = "";
		
		String timestamp = this.runDate + "T00:00:00Z";
		String state = this.opsMgr.getDefaultMissing();


		for (MetricData item : in) {

			service = item.getService();
			hostname = item.getHostname();
			metric = item.getMetric();

			// Filter By endpoint group if belongs to supergroup
			ArrayList<String> groupnames = egpMgr.getGroup(egroupType, hostname, service);

			for (String groupname : groupnames) {
				if (ggpMgr.checkSubGroup(groupname) == true) {
					endpointGroup = groupname;
					found.add(new Tuple4<String, String, String, String>(endpointGroup, service, hostname, metric));
				}

			}

			

		}

		
		// Clone expected set to missing (because missing is going to be mutated after subtraction
		Set<Tuple4<String,String,String,String>> missing = new HashSet<Tuple4<String,String,String,String>>(this.expected);
		// The result of the subtraction is in missing set
		missing.removeAll(found);
		
		
		
		
		// For each item in missing create a missing metric data entry
		for (Tuple4<String, String, String, String> item:missing){
			StatusMetric mn = new StatusMetric();
			// Create a StatusMetric output
			// Grab the timestamp to generate the date and time integer fields
			// that are exclusively used in datastore for indexing
			String timestamp2 = timestamp.split("Z")[0];
			String[] tsToken = timestamp2.split("T");
			int dateInt = Integer.parseInt(tsToken[0].replace("-", ""));
			int timeInt = Integer.parseInt(tsToken[1].replace(":",""));
			mn.setGroup(item.f0);
			mn.setService(item.f1);
			mn.setHostname(item.f2);
			mn.setMetric(item.f3);
			mn.setStatus(state);
			mn.setMessage("");
			mn.setSummary("");
			mn.setTimestamp(timestamp);
			mn.setDateInt(dateInt);
			mn.setTimeInt(timeInt);
			
			out.collect(mn);
			
			
		}
		
		

	}

}
