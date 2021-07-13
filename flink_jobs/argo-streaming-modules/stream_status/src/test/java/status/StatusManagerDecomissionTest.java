package status;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;



import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import status.StatusManager.StatusNode;
import sync.EndpointGroupManagerV2;
import sync.EndpointGroupManagerV2Test;

public class StatusManagerDecomissionTest {

	
	public JsonObject getJSON (String jsonSTR) {
		

		// Gather message from json
		JsonParser jsonParser = new JsonParser();
		// parse the json root object
		JsonObject jRoot = jsonParser.parse(jsonSTR).getAsJsonObject();
		return jRoot;
	}
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", StatusManagerDecomissionTest.class.getResource("/ops/ap1.json"));
		assertNotNull("Test file missing",
				EndpointGroupManagerV2Test.class.getResource("/avro/group_endpoints_v2.avro"));
		assertNotNull("Test file missing",
				EndpointGroupManagerV2Test.class.getResource("/avro/gp_day01.avro"));
		assertNotNull("Test file missing",
				EndpointGroupManagerV2Test.class.getResource("/avro/gp_day02.avro"));	
		assertNotNull("Test file missing",
				EndpointGroupManagerV2Test.class.getResource("/avro/gp_day03.avro"));	
	}

	@Test
	public void test() throws URISyntaxException, IOException, ParseException {
		
		
		
		// Prepare Resource File
		URL resAPSJsonFile = StatusManagerDecomissionTest.class.getResource("/ops/ap1.json");
		File jsonAPSFile = new File(resAPSJsonFile.toURI());

		URL resOPSJsonFile = StatusManagerDecomissionTest.class.getResource("/ops/EGI-algorithm.json");
		File jsonOPSFile = new File(resOPSJsonFile.toURI());

		URL resEGPAvroFile = StatusManagerDecomissionTest.class.getResource("/avro/gp_day01.avro");
		File avroEGPFile = new File(resEGPAvroFile.toURI());
		
		

		URL resMPSAvroFile = StatusManagerDecomissionTest.class.getResource("/avro/poem_sync_2017_03_02.avro");
		File avroMPSFile = new File(resMPSAvroFile.toURI());
		
		URL resDownAvroFile = StatusManagerDecomissionTest.class.getResource("/avro/downtimes_03.avro");
		File avroDownFile = new File(resDownAvroFile.toURI());

		StatusManager sm = new StatusManager();
		sm.setReport("Critical");
		sm.loadAllFiles("2019-06-01", avroDownFile, avroEGPFile, avroMPSFile, jsonAPSFile, jsonOPSFile);
		
		// Prepare Resource File
		URL resAvroFile = EndpointGroupManagerV2Test.class.getResource("/avro/gp_day01.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManagerV2 geDay01 = new EndpointGroupManagerV2();
		// Test loading file for day3 - topology change
		geDay01.loadAvro(avroFile);
		
		Date ts = sm.fromZulu("2017-06-01T00:00:00Z");
		int status = sm.ops.getIntStatus("OK");
		
		ArrayList<String> groupList = geDay01.getGroupList();
		for (String group : groupList) {
			 sm.addNewGroup(group, status, ts);
		}
		
		
		
		
		// Prepare Resource File
		resAvroFile = EndpointGroupManagerV2Test.class.getResource("/avro/gp_day03.avro");
		avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManagerV2 geDay03 = new EndpointGroupManagerV2();
		// Test loading file for day3 - topology change
		geDay03.loadAvro(avroFile);
		
		ArrayList<String> lost = geDay01.compareToBeRemoved(geDay03);
	
		
		// Check that the items to be removed exists in the current status tree
		for (String item : lost) {
			 String[] tokens = item.split(",");
			 // check only for services "CREAM-CE" "ARC-CE"
			 String group = tokens[1];
			 String service = tokens[2];
			 String hostname = tokens[3];
			 if (service.equals("ARC-CE") || service.contentEquals("CREAM-CE")){
				 assertTrue(sm.hasEndpoint(group, service, hostname));
			 } 
		}
		
		sm.updateTopology(geDay03);
		
		// Check that the items to be removed dont exist in tree now
		for (String item : lost) {
			 String[] tokens = item.split(",");
			 // check only for services "CREAM-CE" "ARC-CE"
			 String group = tokens[1];
			 String service = tokens[2];
			 String hostname = tokens[3];
			 if (service.equals("ARC-CE") || service.contentEquals("CREAM-CE")){
				 assertFalse(sm.hasEndpoint(group, service, hostname));
			 } 
		}
		
		
	}

}
