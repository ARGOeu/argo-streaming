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

public class StatusManagerTest {

	
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
		
		URL resDownAvroFile = StatusManagerTest.class.getResource("/avro/downtimes_03.avro");
		File avroDownFile = new File(resDownAvroFile.toURI());

		StatusManager sm = new StatusManager();
                sm.setLooseInterval(1440);
                sm.setStrictInterval(1440);
		sm.setReport("Critical");
		sm.loadAllFiles("2017-03-03", avroDownFile, avroEGPFile, avroMPSFile, jsonAPSFile, jsonOPSFile);

		
		Date ts1 = sm.fromZulu("2017-03-03T00:00:00Z");
		
		sm.addNewGroup("GR-01-AUTH",sm.ops.getIntStatus("OK"), ts1);
		ArrayList<String> list = sm.setStatus("GR-01-AUTH", "CREAM-CE", "cream01.grid.auth.gr", "emi.cream.CREAMCE-JobCancel",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T00:00:00Z","sum1","msg1");
		ArrayList<String> list2 = sm.setStatus("GR-01-AUTH","CREAM-CE", "cream01.grid.auth.gr", "eu.egi.CREAM-IGTF", "WARNING",
				"mon01.argo.eu", "2017-03-03T05:00:00Z","sum2","msg2");
		ArrayList<String> list3 = sm.setStatus("GR-01-AUTH","CREAM-CE", "cream01.grid.auth.gr", "emi.cream.CREAMCE-JobCancel", "OK",
				"mon01.argo.eu", "2017-03-03T09:00:00Z","sum3","msg3");
		ArrayList<String> list4 = sm.setStatus("GR-01-AUTH","CREAM-CE", "cream01.grid.auth.gr", "eu.egi.CREAM-IGTF", "OK",
				"mon01.argo.eu", "2017-03-03T15:00:00Z","sum4","msg4");

		
		Gson gson = new Gson();

		// Gather message from json
		JsonParser jsonParser = new JsonParser();
		// parse the json root object
		JsonElement jRoot = jsonParser.parse(list4.get(0));

		String jproc = jRoot.getAsJsonObject().get("ts_processed").getAsString();
		StatusEvent evnt = new StatusEvent("Critical","metric","20170303","GR-01-AUTH", "CREAM-CE", "cream01.grid.auth.gr",
				"eu.egi.CREAM-IGTF", "OK", "mon01.argo.eu", "2017-03-03T15:00:00Z", jproc,"WARNING","2017-03-03T05:00:00Z", "false","sum4","msg4","false");
		
		evnt.setStatusMetric(new String[] {"OK","WARNING","2017-03-03T15:00:00Z","2017-03-03T05:00:00Z"});
	
		
		
		assertTrue(gson.toJson(evnt).equals(list4.get(0)));

		
		
		
		sm.addNewGroup("UKI-LT2-IC-HEP",sm.ops.getIntStatus("OK"), ts1);
		

		// This should create 4 events
		ArrayList<String> elist01 = sm.setStatus("UKI-LT2-IC-HEP", "CREAM-CE", "ceprod05.grid.hep.ph.ic.ac.uk", "emi.cream.CREAMCE-JobCancel",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T11:00:00Z","sum_A","msg_A");
		assertTrue(elist01.size()==4);
		JsonObject j01 = getJSON(elist01.get(0));
		JsonObject j02 = getJSON(elist01.get(1));
		assertTrue(j01.get("type").getAsString().equals("metric"));
		assertTrue(j02.get("type").getAsString().equals("endpoint"));
		assertTrue(j01.get("ts_monitored").getAsString().equals("2017-03-03T11:00:00Z"));
		assertTrue(j02.get("ts_monitored").getAsString().equals("2017-03-03T11:00:00Z"));
		assertTrue(j01.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j02.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j01.get("hostname").getAsString().equals("ceprod05.grid.hep.ph.ic.ac.uk"));
		assertTrue(j02.get("hostname").getAsString().equals("ceprod05.grid.hep.ph.ic.ac.uk"));
		assertTrue(j01.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j02.get("status").getAsString().equals("CRITICAL"));
		
		assertTrue(j01.get("summary").getAsString().equals("sum_A"));
		assertTrue(j01.get("message").getAsString().equals("msg_A"));
		assertTrue(j02.get("summary").getAsString().equals("sum_A"));
		assertTrue(j02.get("message").getAsString().equals("msg_A"));
		
		ArrayList<String> elist02 = sm.setStatus("UKI-LT2-IC-HEP", "CREAM-CE", "ceprod06.grid.hep.ph.ic.ac.uk", "emi.cream.CREAMCE-JobCancel",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T12:00:00Z","sum_B","msg_B");
		
		assertTrue(elist02.size()==4);
		j01 = getJSON(elist02.get(0));
		j02 = getJSON(elist02.get(1));
		assertTrue(j01.get("type").getAsString().equals("metric"));
		assertTrue(j02.get("type").getAsString().equals("endpoint"));
		assertTrue(j01.get("ts_monitored").getAsString().equals("2017-03-03T12:00:00Z"));
		assertTrue(j02.get("ts_monitored").getAsString().equals("2017-03-03T12:00:00Z"));
		assertTrue(j01.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j02.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j01.get("hostname").getAsString().equals("ceprod06.grid.hep.ph.ic.ac.uk"));
		assertTrue(j02.get("hostname").getAsString().equals("ceprod06.grid.hep.ph.ic.ac.uk"));
		assertTrue(j01.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j02.get("status").getAsString().equals("CRITICAL"));
		
		assertTrue(j01.get("summary").getAsString().equals("sum_B"));
		assertTrue(j01.get("message").getAsString().equals("msg_B"));
		assertTrue(j02.get("summary").getAsString().equals("sum_B"));
		assertTrue(j02.get("message").getAsString().equals("msg_B"));
		
		
		ArrayList<String> elist03 = sm.setStatus("UKI-LT2-IC-HEP", "CREAM-CE", "ceprod07.grid.hep.ph.ic.ac.uk", "emi.cream.CREAMCE-JobCancel",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T14:00:00Z","sum_C","msg_C");
		
		assertTrue(elist03.size()==4);
		j01 = getJSON(elist03.get(0));
		j02 = getJSON(elist03.get(1));
		assertTrue(j01.get("type").getAsString().equals("metric"));
		assertTrue(j02.get("type").getAsString().equals("endpoint"));
		assertTrue(j01.get("ts_monitored").getAsString().equals("2017-03-03T14:00:00Z"));
		assertTrue(j02.get("ts_monitored").getAsString().equals("2017-03-03T14:00:00Z"));
		assertTrue(j01.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j02.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j01.get("hostname").getAsString().equals("ceprod07.grid.hep.ph.ic.ac.uk"));
		assertTrue(j02.get("hostname").getAsString().equals("ceprod07.grid.hep.ph.ic.ac.uk"));
		assertTrue(j01.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j02.get("status").getAsString().equals("CRITICAL"));
		
		assertTrue(j01.get("summary").getAsString().equals("sum_C"));
		assertTrue(j01.get("message").getAsString().equals("msg_C"));
		assertTrue(j02.get("summary").getAsString().equals("sum_C"));
		assertTrue(j02.get("message").getAsString().equals("msg_C"));
		// This should create 3 events metric,endpoint and service as all services endpoints turned into critical
		ArrayList<String> elist04 = sm.setStatus("UKI-LT2-IC-HEP", "CREAM-CE", "ceprod08.grid.hep.ph.ic.ac.uk", "emi.cream.CREAMCE-JobCancel",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T16:00:00Z","sum_D","msg_D");
		
		assertTrue(elist04.size()==4);
		j01 = getJSON(elist04.get(0));
		j02 = getJSON(elist04.get(1));
		JsonObject j03 = getJSON(elist04.get(2));
		
		assertTrue(j01.get("type").getAsString().equals("metric"));
		assertTrue(j02.get("type").getAsString().equals("endpoint"));
		assertTrue(j03.get("type").getAsString().equals("service"));
		assertTrue(j01.get("ts_monitored").getAsString().equals("2017-03-03T16:00:00Z"));
		assertTrue(j02.get("ts_monitored").getAsString().equals("2017-03-03T16:00:00Z"));
		assertTrue(j03.get("ts_monitored").getAsString().equals("2017-03-03T16:00:00Z"));
		assertTrue(j01.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j02.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j03.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j01.get("hostname").getAsString().equals("ceprod08.grid.hep.ph.ic.ac.uk"));
		assertTrue(j02.get("hostname").getAsString().equals("ceprod08.grid.hep.ph.ic.ac.uk"));
		assertTrue(j03.get("hostname").getAsString().equals("ceprod08.grid.hep.ph.ic.ac.uk"));
		assertTrue(j01.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j02.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j03.get("status").getAsString().equals("CRITICAL"));
		
		assertTrue(j01.get("summary").getAsString().equals("sum_D"));
		assertTrue(j01.get("message").getAsString().equals("msg_D"));
		assertTrue(j02.get("summary").getAsString().equals("sum_D"));
		assertTrue(j02.get("message").getAsString().equals("msg_D"));
		assertTrue(j03.get("summary").getAsString().equals("sum_D"));
		assertTrue(j03.get("message").getAsString().equals("msg_D"));
		
		// Site remains ok due to the ARC-CE service. 
		// Turn ARC-CE service to Critical 
		
		
		// This should create 2 events metric
		ArrayList<String> elist05 = sm.setStatus("UKI-LT2-IC-HEP", "ARC-CE", "cetest01.grid.hep.ph.ic.ac.uk", "org.nordugrid.ARC-CE-sw-csh",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T19:00:00Z","sum_E","msg_E");
		assertTrue(elist05.size()==4);
		j01 = getJSON(elist05.get(0));
		j02 = getJSON(elist05.get(1));
		
		
		
		assertTrue(j01.get("type").getAsString().equals("metric"));
		assertTrue(j02.get("type").getAsString().equals("endpoint"));
		assertTrue(j01.get("ts_monitored").getAsString().equals("2017-03-03T19:00:00Z"));
		assertTrue(j02.get("ts_monitored").getAsString().equals("2017-03-03T19:00:00Z"));
		assertTrue(j01.get("metric").getAsString().equals("org.nordugrid.ARC-CE-sw-csh"));
		assertTrue(j02.get("metric").getAsString().equals("org.nordugrid.ARC-CE-sw-csh"));
		assertTrue(j01.get("hostname").getAsString().equals("cetest01.grid.hep.ph.ic.ac.uk"));
		assertTrue(j02.get("hostname").getAsString().equals("cetest01.grid.hep.ph.ic.ac.uk"));
		
		assertTrue(j01.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j02.get("status").getAsString().equals("CRITICAL"));
		
		assertTrue(j01.get("summary").getAsString().equals("sum_E"));
		assertTrue(j01.get("message").getAsString().equals("msg_E"));
		assertTrue(j02.get("summary").getAsString().equals("sum_E"));
		assertTrue(j02.get("message").getAsString().equals("msg_E"));
	
		
		// This should create 4 events metric,endpoint,service and finally endpoint group (the whole site)
		ArrayList<String> elist06 = sm.setStatus("UKI-LT2-IC-HEP", "ARC-CE", "cetest02.grid.hep.ph.ic.ac.uk", "org.nordugrid.ARC-CE-sw-csh",
				"CRITICAL", "mon01.argo.eu", "2017-03-03T21:30:00Z","sum_X","msg_X");
		
		
		assertTrue(elist06.size()==4);
		j01 = getJSON(elist06.get(0));
		j02 = getJSON(elist06.get(1));
		j03 = getJSON(elist06.get(2));
		JsonObject j04 = getJSON(elist06.get(3));
		
		
		
		assertTrue(j01.get("type").getAsString().equals("metric"));
		assertTrue(j02.get("type").getAsString().equals("endpoint"));
		assertTrue(j03.get("type").getAsString().equals("service"));
		assertTrue(j04.get("type").getAsString().equals("endpoint_group"));
		assertTrue(j01.get("ts_monitored").getAsString().equals("2017-03-03T21:30:00Z"));
		assertTrue(j02.get("ts_monitored").getAsString().equals("2017-03-03T21:30:00Z"));
		assertTrue(j03.get("ts_monitored").getAsString().equals("2017-03-03T21:30:00Z"));
		assertTrue(j04.get("ts_monitored").getAsString().equals("2017-03-03T21:30:00Z"));
		assertTrue(j01.get("metric").getAsString().equals("org.nordugrid.ARC-CE-sw-csh"));
		assertTrue(j02.get("metric").getAsString().equals("org.nordugrid.ARC-CE-sw-csh"));
		assertTrue(j03.get("metric").getAsString().equals("org.nordugrid.ARC-CE-sw-csh"));
		assertTrue(j04.get("metric").getAsString().equals("org.nordugrid.ARC-CE-sw-csh"));
		assertTrue(j01.get("hostname").getAsString().equals("cetest02.grid.hep.ph.ic.ac.uk"));
		assertTrue(j02.get("hostname").getAsString().equals("cetest02.grid.hep.ph.ic.ac.uk"));
		assertTrue(j03.get("hostname").getAsString().equals("cetest02.grid.hep.ph.ic.ac.uk"));
		assertTrue(j04.get("hostname").getAsString().equals("cetest02.grid.hep.ph.ic.ac.uk"));
		assertTrue(j01.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j02.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j03.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j03.get("status").getAsString().equals("CRITICAL"));
		assertTrue(j01.get("summary").getAsString().equals("sum_X"));
		assertTrue(j01.get("message").getAsString().equals("msg_X"));
		assertTrue(j02.get("summary").getAsString().equals("sum_X"));
		assertTrue(j02.get("message").getAsString().equals("msg_X"));
		assertTrue(j03.get("summary").getAsString().equals("sum_X"));
		assertTrue(j03.get("message").getAsString().equals("msg_X"));
		assertTrue(j04.get("summary").getAsString().equals("sum_X"));
		assertTrue(j04.get("message").getAsString().equals("msg_X"));
		
		// This should create 4 events metric,endpoint,service and finally endpoint group (the whole site)
		ArrayList<String> elist07 = sm.setStatus("UKI-LT2-IC-HEP", "CREAM-CE", "ceprod05.grid.hep.ph.ic.ac.uk", "emi.cream.CREAMCE-JobCancel",
				"OK", "mon01.argo.eu", "2017-03-03T22:30:00Z","","");
		
		
		
		assertTrue(elist07.size()==4);
		j01 = getJSON(elist07.get(0));
		j02 = getJSON(elist07.get(1));
		j03 = getJSON(elist07.get(2));
		j04 = getJSON(elist07.get(3));
		
		// check list of metric statuses and metric names included in the endpoint
		assertTrue(j02.get("metric_statuses").toString().equals("[\"OK\",\"OK\",\"OK\",\"OK\",\"OK\",\"OK\"]"));
		assertTrue(j02.get("metric_names").toString().equals("[\"emi.cream.CREAMCE-ServiceInfo\",\"emi.cream.CREAMCE-JobCancel\",\"hr.srce.CREAMCE-CertLifetime\",\"eu.egi.CREAM-IGTF\",\"emi.cream.CREAMCE-JobPurge\",\"emi.cream.CREAMCE-AllowedSubmission\"]"));
		
		// check if endpoint groups have been captured
		assertTrue(j04.get("group_endpoints").toString().equals("[\"cetest01.grid.hep.ph.ic.ac.uk\",\"cetest02.grid.hep.ph.ic.ac.uk\",\"bdii.grid.hep.ph.ic.ac.uk\",\"ceprod08.grid.hep.ph.ic.ac.uk\",\"ceprod06.grid.hep.ph.ic.ac.uk\",\"ceprod07.grid.hep.ph.ic.ac.uk\",\"ceprod05.grid.hep.ph.ic.ac.uk\"]"));
		assertTrue(j04.get("group_services").toString().equals("[\"ARC-CE\",\"ARC-CE\",\"Site-BDII\",\"CREAM-CE\",\"CREAM-CE\",\"CREAM-CE\",\"CREAM-CE\"]"));
		assertTrue(j04.get("group_statuses").toString().equals("[\"CRITICAL\",\"CRITICAL\",\"OK\",\"CRITICAL\",\"CRITICAL\",\"CRITICAL\",\"OK\"]"));
		assertTrue(j01.get("type").getAsString().equals("metric"));
		assertTrue(j02.get("type").getAsString().equals("endpoint"));
		assertTrue(j03.get("type").getAsString().equals("service"));
		assertTrue(j04.get("type").getAsString().equals("endpoint_group"));
		assertTrue(j01.get("ts_monitored").getAsString().equals("2017-03-03T22:30:00Z"));
		assertTrue(j02.get("ts_monitored").getAsString().equals("2017-03-03T22:30:00Z"));
		assertTrue(j03.get("ts_monitored").getAsString().equals("2017-03-03T22:30:00Z"));
		assertTrue(j04.get("ts_monitored").getAsString().equals("2017-03-03T22:30:00Z"));
		assertTrue(j01.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j02.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j03.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j04.get("metric").getAsString().equals("emi.cream.CREAMCE-JobCancel"));
		assertTrue(j01.get("hostname").getAsString().equals("ceprod05.grid.hep.ph.ic.ac.uk"));
		assertTrue(j02.get("hostname").getAsString().equals("ceprod05.grid.hep.ph.ic.ac.uk"));
		assertTrue(j03.get("hostname").getAsString().equals("ceprod05.grid.hep.ph.ic.ac.uk"));
		assertTrue(j04.get("hostname").getAsString().equals("ceprod05.grid.hep.ph.ic.ac.uk"));
		assertTrue(j01.get("status").getAsString().equals("OK"));
		assertTrue(j02.get("status").getAsString().equals("OK"));
		assertTrue(j03.get("status").getAsString().equals("OK"));
		assertTrue(j03.get("status").getAsString().equals("OK"));
		
		assertTrue(j01.get("summary").getAsString().equals(""));
		assertTrue(j01.get("message").getAsString().equals(""));
		assertTrue(j02.get("summary").getAsString().equals(""));
		assertTrue(j02.get("message").getAsString().equals(""));
		assertTrue(j03.get("summary").getAsString().equals(""));
		assertTrue(j03.get("message").getAsString().equals(""));


		// downtime affected should not create event
		sm.addNewGroup("GR-07-UOI-HEPLAB",sm.ops.getIntStatus("OK"), ts1);
		ArrayList<String> elist08 = sm.setStatus("GR-07-UOI-HEPLAB", "CREAM-CE", "grid01.physics.uoi.gr", "emi.cream.CREAMCE-JobCancel", "CRITICAL", "mon01.argo.eu", "2017-03-03T22:45:00Z", "", "");
		assertEquals(0,elist08.size());
		
		// downtime affected should not create events
		sm.addNewGroup("ru-Moscow-FIAN-LCG2",sm.ops.getIntStatus("OK"), ts1);
		ArrayList<String> elist09 = sm.setStatus("ru-Moscow-FIAN-LCG2", "Site-BDII", "ce1.grid.lebedev.ru", "org.bdii.Freshness", "CRITICAL", "mon01.argo.eu", "2017-03-03T22:55:00Z", "", "");
		assertEquals(0,elist09.size());
		
		// not affected site-bdii should generate events
		sm.addNewGroup("WUT",sm.ops.getIntStatus("OK"), ts1);
		ArrayList<String> elist10 = sm.setStatus("WUT", "Site-BDII", "bdii.if.pw.edu.pl", "org.bdii.Freshness", "CRITICAL",  "mon01.argo.eu", "2017-03-03T23:00:00Z", "", "");
		assertEquals(4,elist10.size());
	}

}
