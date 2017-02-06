package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;

import ops.OpsManagerTest;

import org.junit.BeforeClass;
import org.junit.Test;

import junitx.framework.ListAssert;

public class AvailabilityProfilesTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", AvailabilityProfilesTest.class.getResource("/ops/ap1.json"));
	}

	@Test
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resJsonFile = OpsManagerTest.class.getResource("/ops/ap1.json");
		File jsonFile = new File(resJsonFile.toURI());
		// Instatiate class
		AvailabilityProfiles avp = new AvailabilityProfiles();
		avp.clearProfiles();
		avp.loadJson(jsonFile);

		// Check that only one availability profile was loaded
		assertEquals("Only 1 av profile present", avp.getAvProfiles().size(), 1);

		ArrayList<String> expApList = new ArrayList<String>();
		expApList.add("ap1");

		// Check the profile list is correct
		assertEquals("Profile list check", avp.getAvProfiles(), expApList);

		// Check the profile namespace
		assertEquals("Profile namespace", avp.getProfileNamespace("ap1"), "test");

		// Check the profile groupType
		assertEquals("Profile group type", avp.getProfileGroupType("ap1"), "sites");

		// Set the expected profile groups
		ArrayList<String> expGroups = new ArrayList<String>();
		expGroups.add("information");
		expGroups.add("compute");
		expGroups.add("storage");
		// Check the available group list
		ListAssert.assertEquals("Profile Groups", avp.getProfileGroups("ap1"), expGroups);

		// Check compute group service list
		ArrayList<String> expServices = new ArrayList<String>();
		expServices.add("GRAM5");
		expServices.add("QCG.Computing");
		expServices.add("ARC-CE");
		expServices.add("unicore6.TargetSystemFactory");
		expServices.add("CREAM-CE");

		ListAssert.assertEquals("compute service list", avp.getProfileGroupServices("ap1", "compute"), expServices);

		// Check storage group service list
		expServices = new ArrayList<String>();
		expServices.add("SRM");
		expServices.add("SRMv2");
		ListAssert.assertEquals("storage service list", avp.getProfileGroupServices("ap1", "storage"), expServices);

		// Check storage group service list
		expServices = new ArrayList<String>();
		expServices.add("Site-BDII");
		ListAssert.assertEquals("accounting  list", avp.getProfileGroupServices("ap1", "information"), expServices);

		// Check Various Service Instances operation
		assertEquals("group compute: CREAM-CE op", avp.getProfileGroupServiceOp("ap1", "compute", "CREAM-CE"), "OR");
		assertEquals("group compute: ARC-CE op", avp.getProfileGroupServiceOp("ap1", "compute", "ARC-CE"), "OR");
		assertEquals("group storage: SRMv2 op", avp.getProfileGroupServiceOp("ap1", "storage", "SRM"), "OR");
		assertEquals("group storage: SRM op", avp.getProfileGroupServiceOp("ap1", "storage", "SRMv2"), "OR");
		assertEquals("group information: Site-BDII op", avp.getProfileGroupServiceOp("ap1", "information", "Site-BDII"),
				"OR");
		assertEquals("get group by service: ", avp.getGroupByService("ap1", "CREAM-CE"), "compute");
		assertEquals("get group by service: ", avp.getGroupByService("ap1", "SRMv2"), "storage");
		// we check for an unexpected operation
		assertNotEquals("group compute: CREAM-CE op", avp.getProfileGroupServiceOp("ap1", "compute", "CREAM-CE"),
				"AND");
		assertNotEquals("group compute: CREAM-CE op", avp.getProfileGroupServiceOp("ap1", "informationss", "CREAM-CE"),
				"AND");
		assertNotEquals("group compute: CREAM-CE op", avp.getProfileGroupServiceOp("ap1", "storage", "CREAM-CE"),
				"FOO");
		// check for metric profile operations and total operation
		assertEquals("metric profile operations: AND", avp.getMetricOp("ap1"), "AND");
		assertEquals("total profile operations: AND", avp.getMetricOp("ap1"), "AND");

	}

}
