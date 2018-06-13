package argo.batch;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;


import sync.EndpointGroupManager;
import argo.avro.MetricData;

public class MultipleServiceGroupsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", MultipleServiceGroupsTest.class.getResource("/avro/group_example.avro"));
	}

	@Test
	public void test() throws URISyntaxException, IOException, ParseException {
		// Prepare Resource File
		URL resAvroFile = MultipleServiceGroupsTest.class.getResource("/avro/group_example.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManager ge = new EndpointGroupManager();
		// Test loading file
		ge.loadAvro(avroFile);
		assertNotNull("File Loaded", ge);

		// Create MetricData
		MetricData md1 = new MetricData();
		md1.setHostname("hostA");
		md1.setService("service.typeA");

		ArrayList<String> groups = ge.getGroup("GROUPA", md1.getHostname(), md1.getService());

		ArrayList<MonData> monList = new ArrayList<MonData>();

		for (String group : groups) {
			MonData mn = new MonData();
			mn.setGroup(group);
			mn.setHostname(md1.getHostname());
			mn.setService(md1.getService());
			monList.add(mn);
		}

		String expOut = "[(SH,service.typeA,hostA,,,,,,), " + "(SH_N,service.typeA,hostA,,,,,,), "
				+ "(PROV,service.typeA,hostA,,,,,,), " + "(D,service.typeA,hostA,,,,,,), "
				+ "(SC,service.typeA,hostA,,,,,,), " + "(PR,service.typeA,hostA,,,,,,), "
				+ "(SH_R,service.typeA,hostA,,,,,,), " + "(OP,service.typeA,hostA,,,,,,), "
				+ "(SH_L,service.typeA,hostA,,,,,,), " + "(GT,service.typeA,hostA,,,,,,), "
				+ "(ORG_C,service.typeA,hostA,,,,,,)]";

		assertEquals("multiple group test",expOut,Arrays.toString(monList.toArray()));

	}

}
