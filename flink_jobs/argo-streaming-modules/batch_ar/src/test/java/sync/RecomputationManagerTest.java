package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;



import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecomputationManagerTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", RecomputationManagerTest.class.getResource("/ops/recomp.json"));
	}

	@Test
	public void test() throws URISyntaxException, ParseException, IOException {
		// Prepare Resource File
		URL resJsonFile = RecomputationManagerTest.class.getResource("/ops/recomp.json");
		File jsonFile = new File(resJsonFile.toURI());

		RecomputationManager recMgr = new RecomputationManager();
		recMgr.loadJson(jsonFile);
		

		assertEquals(recMgr.isExcluded("GR-01-AUTH"), true);
		assertEquals(recMgr.isExcluded("HG-03-AUTH"), true);
		assertEquals(recMgr.isExcluded("GR-04-IASA"), false);

		// Check period functionality
		ArrayList<Map<String,String>> gr01list = new ArrayList<Map<String,String>>();
		ArrayList<Map<String,String>> siteAlist = new ArrayList<Map<String,String>>();
		ArrayList<Map<String,String>> siteBlist = new ArrayList<Map<String,String>>();
		ArrayList<Map<String,String>> siteClist = new ArrayList<Map<String,String>>();
		
		Map<String,String> gr01map = new HashMap<String,String>();
		
		Map<String,String> siteA1map = new HashMap<String, String>();
		Map<String,String> siteA2map = new HashMap<String, String>();
		
		
		
		Map<String,String> siteBmap = new HashMap<String,String>();
		Map<String,String> siteCmap = new HashMap<String,String>();
	
		// Check period functionality
		
		gr01map.put("start", "2013-12-08T12:03:44Z");
		gr01map.put("end", "2013-12-10T12:03:44Z");
		
		siteA1map.put("start", "2013-12-08T12:03:44Z");
		siteA1map.put("end", "2013-12-08T13:03:44Z");
		
		siteA2map.put("start", "2013-12-08T16:03:44Z");
		siteA2map.put("end", "2013-12-08T18:03:44Z");
		
		siteBmap.put("start", "2013-12-08T12:03:44Z");
		siteBmap.put("end", "2013-12-08T13:03:44Z");
		
		siteCmap.put("start", "2013-12-08T16:03:44Z");
		siteCmap.put("end", "2013-12-08T18:03:44Z");
		
		gr01list.add(gr01map);
	    siteAlist.add(siteA1map);
	    siteAlist.add(siteA2map);
	    siteBlist.add(siteBmap);
	    siteClist.add(siteCmap);
	    
	    Assert.assertEquals(recMgr.getPeriods("GR-01-AUTH", "2013-12-08"),gr01list);
	    Assert.assertEquals(recMgr.getPeriods("SITE-A", "2013-12-08"),siteAlist);
	    Assert.assertEquals(recMgr.getPeriods("SITE-B", "2013-12-08"),siteBlist);
	    
	    // check monitoring exclusions
	    Assert.assertEquals(false,recMgr.isMonExcluded("monA", "2013-12-08T11:03:43Z"));
	    Assert.assertEquals(false,recMgr.isMonExcluded("monA", "2013-12-08T11:03:44Z"));
	    Assert.assertEquals(true,recMgr.isMonExcluded("monA", "2013-12-08T12:06:44Z"));
	    Assert.assertEquals(true,recMgr.isMonExcluded("monA", "2013-12-08T14:05:44Z"));
	    Assert.assertEquals(true,recMgr.isMonExcluded("monA", "2013-12-08T15:02:44Z"));
	    Assert.assertEquals(false,recMgr.isMonExcluded("monA", "2013-12-08T15:03:45Z"));
	    
	    // check monitoring exclusions
	    Assert.assertEquals(false,recMgr.isMonExcluded("monB", "2013-12-08T11:03:43Z"));
	    Assert.assertEquals(false,recMgr.isMonExcluded("monB", "2013-12-08T11:03:44Z"));
	    Assert.assertEquals(false,recMgr.isMonExcluded("monB", "2013-12-08T12:06:44Z"));
	    Assert.assertEquals(false,recMgr.isMonExcluded("monB", "2013-12-08T14:05:44Z"));
	    Assert.assertEquals(false,recMgr.isMonExcluded("monB", "2013-12-08T15:02:44Z"));
	    Assert.assertEquals(false,recMgr.isMonExcluded("monB", "2013-12-08T15:03:45Z"));
	    
	}

}
