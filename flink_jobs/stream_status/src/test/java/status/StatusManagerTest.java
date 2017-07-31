package status;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import sync.EndpointGroups.EndpointItem;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class StatusManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", StatusManagerTest.class.getResource("/ops/ap1.json"));
	}

	@Test
	public void test() throws URISyntaxException, IOException, ParseException {
		// Prepare Resource File
		URL resAPSJsonFile = StatusManagerTest.class.getResource("/ops/ap1.json");
		File jsonAPSFile = new File(resAPSJsonFile.toURI());

		URL resOPSJsonFile = StatusManagerTest.class.getResource("/ops/EGI-algorithm.json");
		File jsonOPSFile = new File(resOPSJsonFile.toURI());

		URL resEGPAvroFile = StatusManagerTest.class.getResource("/avro/group_endpoints_v2.avro");
		File avroEGPFile = new File(resEGPAvroFile.toURI());

		URL resMPSAvroFile = StatusManagerTest.class.getResource("/avro/poem_sync_2017_03_02.avro");
		File avroMPSFile = new File(resMPSAvroFile.toURI());

		StatusManager sm = new StatusManager();
		sm.report="Critical";
		sm.loadAll(avroEGPFile, avroMPSFile, jsonAPSFile, jsonOPSFile);

		Date ts1 = sm.fromZulu("2017-03-03T00:00:00Z");
		Date ts2 = sm.fromZulu("2017-03-03T05:00:00Z");
		Date ts3 = sm.fromZulu("2017-03-03T09:00:00Z");
		Date ts4 = sm.fromZulu("2017-03-03T15:00:00Z");

		sm.construct(sm.ops.getIntStatus("OK"), ts1);
		ArrayList<String> list = sm.setStatus("CREAM-CE", "cream01.grid.auth.gr", "emi.cream.CREAMCE-JobCancel",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T00:00:00Z");
		ArrayList<String> list2 = sm.setStatus("CREAM-CE", "cream01.grid.auth.gr", "eu.egi.CREAM-IGTF", "WARNING",
				"mon01.argo.eu", "2017-03-03T05:00:00Z");
		ArrayList<String> list3 = sm.setStatus("CREAM-CE", "cream01.grid.auth.gr", "emi.cream.CREAMCE-JobCancel", "OK",
				"mon01.argo.eu", "2017-03-03T09:00:00Z");
		ArrayList<String> list4 = sm.setStatus("CREAM-CE", "cream01.grid.auth.gr", "eu.egi.CREAM-IGTF", "OK",
				"mon01.argo.eu", "2017-03-03T15:00:00Z");

		
		Gson gson = new Gson();

		// Gather message from json
		JsonParser jsonParser = new JsonParser();
		// parse the json root object
		JsonElement jRoot = jsonParser.parse(list4.get(0));

		String jproc = jRoot.getAsJsonObject().get("ts_processed").getAsString();
		StatusEvent evnt = new StatusEvent("Critical","metric","20170303","GR-01-AUTH", "CREAM-CE", "cream01.grid.auth.gr",
				"eu.egi.CREAM-IGTF", "OK", "mon01.argo.eu", "2017-03-03T15:00:00Z", jproc,"WARNING","2017-03-03T05:00:00Z");
		
		assertTrue(gson.toJson(evnt).equals(list4.get(0)));

	}

}
