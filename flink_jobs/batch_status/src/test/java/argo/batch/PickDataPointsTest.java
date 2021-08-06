package argo.batch;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import java.net.URL;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.core.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import argo.avro.GroupEndpoint;
import argo.avro.GroupGroup;
import argo.avro.MetricData;
import argo.avro.MetricProfile;
import junit.framework.Assert;
import ops.ThresholdManagerTest;
import sync.AggregationProfileManager;
import sync.EndpointGroupManager;
import sync.GroupGroupManager;
import sync.MetricProfileManager;
import sync.RecomputationManagerTest;

public class PickDataPointsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/avro/group_endpoints_info.avro"));
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/avro/poem_url_services.avro"));
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/ops/ap1.json"));
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/avro/groups_info.avro"));
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/ops/config.json"));
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/ops/EGI-algorithm.json"));
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/ops/recomp.json.flink"));
		assertNotNull("Test file missing", PickDataPointsTest.class.getResource("/ops/EGI-rules.json"));
		

	}

	@Test
	public void test() throws Exception {

		// Prepare Resource File
		URL resAvroFile = PickDataPointsTest.class.getResource("/avro/poem_url_services.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instatiate class
		MetricProfileManager mp = new MetricProfileManager();
		// Test loading file
		mp.loadAvro(avroFile);
		assertNotNull("File Loaded", mp);

		// Prepare Resource File
		URL resJsonFileAp = PickDataPointsTest.class.getResource("/ops/ap1.json");
		File jsonFileAp = new File(resJsonFileAp.toURI());
		// Instatiate class
		AggregationProfileManager avp = new AggregationProfileManager();
		avp.clearProfiles();
		avp.loadJson(jsonFileAp);

		// Prepare Resource File
		URL resAvroFileEndpoint = PickDataPointsTest.class.getResource("/avro/group_endpoints_info.avro");
		File avroFileEndpoint = new File(resAvroFileEndpoint.toURI());
		// Instatiate class
		EndpointGroupManager ge = new EndpointGroupManager();
		// Test loading file
		ge.loadAvro(avroFileEndpoint);
		assertNotNull("File Loaded", ge);

		// Prepare Resource File
		URL resAvroFileGroup = PickDataPointsTest.class.getResource("/avro/groups_info.avro");
		File avroFileGroup = new File(resAvroFileGroup.toURI());
		// Instatiate class
		GroupGroupManager gg = new GroupGroupManager();
		// Test loading file
		gg.loadAvro(avroFileGroup);
		assertNotNull("File Loaded", gg);

		// Prepare Resource File
		URL resJsonFileCfg = PickDataPointsTest.class.getResource("/ops/config.json");
		File jsonFileCfg = new File(resJsonFileCfg.toURI());
		

		// Prepare Resource File
		URL resJsonFileOps = PickDataPointsTest.class.getResource("/ops/EGI-algorithm.json");
		File jsonFileOps = new File(resJsonFileOps.toURI());

		// Prepare Resource File which contains recomputations
		URL resJsonFileRecomp = RecomputationManagerTest.class.getResource("/ops/recomp.json.flink");
		File jsonFileRecomp = new File(resJsonFileRecomp.toURI());

		// Prepare Resource File
		URL thrJsonFile = ThresholdManagerTest.class.getResource("/ops/EGI-rules.json");
		File thrFile = new File(thrJsonFile.toURI());

		// Prepare a local flink execution environment for testing
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

	
		// Create a data set with metric data - some of them are coming from monitoring
		// host bad-mon.example.org
		// which should be excluded during 02:00 and 06:00 period of 2019-05-07
		MetricData md01 = new MetricData("2019-05-07T00:00:00Z", "services.url", "host1.example.foo_11", "check_http",
				"CRITICAL", "mon2", "", "summary", "msg", null);
		MetricData md02 = new MetricData("2019-05-07T01:00:00Z", "services.url", "host1.example.foo_22", "check_http", "OK",
				"mon2", "", "summary", "msg", null);
		MetricData md03 = new MetricData("2019-05-07T03:00:00Z", "services.url", "host2.example.foo_33", "check_http", "OK",
				"mon2", "", "summary", "msg", null);
		MetricData md04 = new MetricData("2019-05-07T03:32:00Z", "services.url", "host2.example.foo_44", "check_http",
				"CRITICAL", "mon2", "", "summary", "msg", null);
		MetricData md05 = new MetricData("2019-05-07T04:00:00Z", "services.url", "host3.example.foo_55", "check_http", "OK",
				"mon2", "", "summary", "msg", null);
		MetricData md06 = new MetricData("2019-05-07T04:32:00Z", "services.url", "host4.example.foo_66", "check_http",
				"CRITICAL", "mon2", "", "summary", "msg", null);

		// Create a config manager dataset by reading the configfile. This dataset will
		// be used as a broadcast variable
		String cfgStr = new String();
		BufferedReader br = new BufferedReader(new FileReader(jsonFileCfg));
		cfgStr = br.readLine();
		br.close();
		DataSet<String> cfgDS = env.fromElements(cfgStr);

		String opsStr = new String();
		br = new BufferedReader(new FileReader(jsonFileOps));
		opsStr = br.readLine();
		br.close();
		DataSet<String> opsDS = env.fromElements(opsStr);

		String apStr = new String();
		br = new BufferedReader(new FileReader(jsonFileAp));
		apStr = br.readLine();
		br.close();
		DataSet<String> apDS = env.fromElements(apStr);

		String recStr = new String();
		br = new BufferedReader(new FileReader(jsonFileRecomp));
		recStr = br.readLine();
		br.close();
		DataSet<String> recDS = env.fromElements(recStr);
		
		String thrStr = new String();
		br = new BufferedReader(new FileReader(thrFile));
		recStr = br.readLine();
		br.close();
		DataSet<String> thrDS = env.fromElements(thrStr);
		
		
		

		// sync data input: metric profile in avro format
		AvroInputFormat<MetricProfile> mpsAvro = new AvroInputFormat<MetricProfile>(new Path(resAvroFile.toURI()),
				MetricProfile.class);
		DataSet<MetricProfile> mpsDS = env.createInput(mpsAvro);

		// sync data input: endpoint group topology data in avro format
		AvroInputFormat<GroupEndpoint> egpAvro = new AvroInputFormat<GroupEndpoint>(
				new Path(resAvroFileEndpoint.toURI()), GroupEndpoint.class);
		DataSet<GroupEndpoint> egpDS = env.createInput(egpAvro);

		// sync data input: group of group topology data in avro format
		AvroInputFormat<GroupGroup> ggpAvro = new AvroInputFormat<GroupGroup>(new Path(resAvroFileGroup.toURI()),
				GroupGroup.class);
		DataSet<GroupGroup> ggpDS = env.createInput(ggpAvro);

		// Read the initial metric data
		DataSet<MetricData> md = env.fromElements(md01, md02, md03, md04, md05, md06);
		// Clean the metric data by testing the ExcludeMetricData flatmap function with
		// recomputation information to exclude bad-mon01.example.org data from 02:00 to
		// 06:00 (broadcast variable)
		DataSet<StatusMetric> clearMd = md.flatMap(new PickEndpoints(null)).withBroadcastSet(cfgDS, "conf")
				.withBroadcastSet(recDS, "rec").withBroadcastSet(opsDS, "ops").withBroadcastSet(mpsDS, "mps")
				.withBroadcastSet(ggpDS, "ggp").withBroadcastSet(egpDS, "egp").withBroadcastSet(thrDS, "thr")
				.withBroadcastSet(apDS, "aps");

		// collect the final result in a list
		List<StatusMetric> resulted = clearMd.collect();
		
		Assert.assertEquals(5,resulted.size());
		
		String[] expected = new String[] {"URL:host1.example.foo/path/to/service1,DN:foo DN", 
				"URL:host1.example.foo/path/to/service2", 
				"URL:host2.example.foo/path/to/service1",
				"ext.Value:extension1,URL:host2.example.foo/path/to/service2",
				""};
		

		System.out.println(resulted);
		
		for (int i = 0;i<resulted.size();i++) { 
			Assert.assertEquals(expected[i], resulted.get(i).getInfo());
		}

	}

}
