package sync;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import argo.avro.Downtime;
import argo.streaming.SyncParse;

import org.apache.commons.codec.binary.Base64;

public class DowntimeCacheTest {

	private static String[] fileList = { "/base64/downtimes_01.base64", 
			"/base64/downtimes_02.base64",
			"/base64/downtimes_03.base64", 
			"/base64/downtimes_04.base64", 
			"/base64/downtimes_05.base64",
			"/base64/downtimes_06.base64", 
			"/base64/downtimes_07.base64", 
			"/base64/downtimes_08.base64" };
	
	private static String[] dayList = {"2018-05-09",
			"2018-05-10",
			"2018-05-12",
			"2018-05-13",
			"2018-05-14",
			"2018-05-15",
			"2018-05-16",
			"2018-05-17"};
	

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present

		for (String fileName : fileList) {
			assertNotNull("Test file missing", DowntimeCacheTest.class.getResource(fileName));
		}

	}

	@Test
	public void test() throws IOException, URISyntaxException {

		String b64List[] = new String[8];
		Map<String,ArrayList<Downtime>> downtimeMap = new HashMap<String,ArrayList<Downtime>>();
		
		
		int i=0;
		for (String fileName : fileList) {
			b64List[i] = new String(Files
					.readAllBytes(Paths.get(DowntimeCacheTest.class.getResource(fileName).toURI())));
			i++;
		}
		
		i=0;
		for (String b64:b64List) {
			byte[] decoded = Base64.decodeBase64(b64.getBytes("UTF-8"));
			ArrayList<Downtime> dt = SyncParse.parseDowntimes(decoded);
			downtimeMap.put(dayList[i], dt);
			i++;
		}
		
		DowntimeCache dc2 = new DowntimeCache(2);
		// Downtime set arrives for 2018-05-10 add it to the cache
		dc2.addFeed("2018-05-10",downtimeMap.get("2018-05-10"));
		// Downtime set arrives for 2018-05-09 (older than the oldest downtime day in cache 05/10) ignore it
		assertEquals(null,dc2.getDowntimeManager("2018-05-09"));
		assertNotEquals(null,dc2.getDowntimeManager("2018-05-10"));
		dc2.addFeed("2018-05-13",downtimeMap.get("2018-05-13"));
		dc2.addFeed("2018-05-14",downtimeMap.get("2018-05-14"));
		
		// Not cache should contain only 2018-05-13 and 2018-05-14
		assertEquals(null,dc2.getDowntimeManager("2018-05-09"));
		assertEquals(null,dc2.getDowntimeManager("2018-05-10"));
		
		DowntimeManager dm13 = new DowntimeManager();
		DowntimeManager dm14 = new DowntimeManager();
		dm13.loadFromList(downtimeMap.get("2018-05-13"));
		dm14.loadFromList(downtimeMap.get("2018-05-14"));
		
		
	
		assertEquals(dm13.toString(),dc2.getDowntimeManager("2018-05-13").toString());
		assertEquals(dm14.toString(),dc2.getDowntimeManager("2018-05-14").toString());
		
		// Assert that max items are 2
		assertEquals(2,dc2.getMaxItems());
		
		assertEquals("[2018-05-13T00:00:00Z, 2018-05-13T23:59:00Z]",dc2.getDowntimePeriod("2018-05-13", "cluster51.knu.ac.kr", "Site-BDII").toString());
		assertEquals("[2018-05-14T00:00:00Z, 2018-05-14T23:59:00Z]",dc2.getDowntimePeriod("2018-05-14", "cluster51.knu.ac.kr", "Site-BDII").toString());
		assertEquals("[2018-05-13T14:00:00Z, 2018-05-13T23:59:00Z]",dc2.getDowntimePeriod("2018-05-13", "fal-pygrid-30.lancs.ac.uk", "webdav").toString());
		// Insert new element 2018-05-16 that will remove 2018-05-13
		dc2.addFeed("2018-05-16", downtimeMap.get("2018-05-16"));
		assertEquals(null,dc2.getDowntimePeriod("2018-05-13", "fal-pygrid-30.lancs.ac.uk", "webdav"));
		// ..but 2018-05-14 still exists
		assertEquals("[2018-05-14T00:00:00Z, 2018-05-14T23:59:00Z]",dc2.getDowntimePeriod("2018-05-14", "cluster51.knu.ac.kr", "Site-BDII").toString());
		// Insert new element 2018-05-17 that will remove 2018-05-14
		dc2.addFeed("2018-05-17", downtimeMap.get("2018-05-17"));
		assertEquals(null,dc2.getDowntimePeriod("2018-05-14", "cluster51.knu.ac.kr", "Site-BDII"));
	
		DowntimeCache dc3 = new DowntimeCache(3);
		
		// Begin by adding downtime dataset for day 12
		dc3.addFeed("2018-05-12", downtimeMap.get("2018-05-12"));
		// The following days should be ignored (10 and 09)
		dc3.addFeed("2018-05-10", downtimeMap.get("2018-05-10"));
		dc3.addFeed("2018-05-09", downtimeMap.get("2018-05-09"));
		assertEquals(null,dc3.getDowntimeManager("2018-05-09"));
		assertEquals(null,dc3.getDowntimeManager("2018-05-10"));
		
		// Add 17,18,15,16 (also having 12) at one moment 15 will replace 12
		dc3.addFeed("2018-05-17", downtimeMap.get("2018-05-17"));
		dc3.addFeed("2018-05-18", downtimeMap.get("2018-05-18"));
		dc3.addFeed("2018-05-15", downtimeMap.get("2018-05-15"));
		assertEquals(null,dc3.getDowntimeManager("2018-05-12"));
		
		DowntimeManager dm15 = new DowntimeManager();
		DowntimeManager dm17 = new DowntimeManager();
		DowntimeManager dm18 = new DowntimeManager();
		
		dm15.loadFromList(downtimeMap.get("2018-05-15"));
		dm17.loadFromList(downtimeMap.get("2018-05-17"));
		dm18.loadFromList(downtimeMap.get("2018-05-18"));
		
		assertEquals(dm15.toString(),dc3.getDowntimeManager("2018-05-15").toString());
		assertEquals(dm17.toString(),dc3.getDowntimeManager("2018-05-17").toString());
		assertEquals(dm18.toString(),dc3.getDowntimeManager("2018-05-18").toString());
		
		
		
		
	}

}