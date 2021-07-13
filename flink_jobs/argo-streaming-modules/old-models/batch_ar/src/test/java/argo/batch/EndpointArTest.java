package argo.batch;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;


import org.junit.BeforeClass;
import org.junit.Test;


import ops.DIntegrator;
import ops.OpsManager;

public class EndpointArTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", ServiceArTest.class.getResource("/ops/EGI-algorithm.json"));
	}

	@Test
	public void test() throws URISyntaxException, IOException, ParseException {
		// Load operations manager
		// Prepare Resource File
		URL resJsonFile = ServiceArTest.class.getResource("/ops/EGI-algorithm.json");
		File JsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		OpsManager opsMgr = new OpsManager();
		// Test loading file
		opsMgr.loadJson(JsonFile);

		// Assert the state strings to integer mappings ("OK"=>0, "MISSING"=>3
		// etc)
		assertEquals(0, opsMgr.getIntStatus("OK"));
		assertEquals(1, opsMgr.getIntStatus("WARNING"));
		assertEquals(3, opsMgr.getIntStatus("MISSING"));
		assertEquals(4, opsMgr.getIntStatus("CRITICAL"));

		// Add endpoint Timelines
		int[] endpoint01s = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
				1, 1, 1, 1, 1, 1, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

		int[] endpoint02s = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

		MonTimeline endp01 = new MonTimeline("SITEA", "CREAM-CE", "cream01.endpointA.foo", "");
		endp01.setTimeline(endpoint01s);

		MonTimeline endp02 = new MonTimeline("SITEA", "SRM", "srm01.endpointB.foo", "");
		endp02.setTimeline(endpoint02s);

		ArrayList<MonTimeline> endpDS = new ArrayList<MonTimeline>();
		endpDS.add(endp01);
	    endpDS.add(endp02);
		
	    ArrayList<EndpointAR> resultDS = new ArrayList<EndpointAR>();
	    
		String runDate = "2017-07-01";
		String report = "reportFOO";
		

		
		for (MonTimeline item : endpDS) {

			DIntegrator dAR = new DIntegrator();
			dAR.calculateAR(item.getTimeline(),opsMgr); 
			
			int runDateInt = Integer.parseInt(runDate.replace("-", ""));
			
			EndpointAR result = new EndpointAR(runDateInt,report,item.getHostname(),item.getService(),item.getGroup(),dAR.availability,dAR.reliability,dAR.up_f,dAR.unknown_f,dAR.down_f);
			resultDS.add(result);
		}
		
		String expEndpoint01 = "(20170701,reportFOO,cream01.endpointA.foo,CREAM-CE,SITEA,87.5,87.5,0.875,0.0,0.0)";
		String expEndpoint02 = "(20170701,reportFOO,srm01.endpointB.foo,SRM,SITEA,100.0,100.0,1.0,0.0,0.0)";
		
		assertEquals("check cream-ce service results",expEndpoint01,resultDS.get(0).toString());
		assertEquals("check srm service results",expEndpoint02,resultDS.get(1).toString());
		

	}

}
