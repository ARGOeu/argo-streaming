package sync;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;




import org.junit.BeforeClass;
import org.junit.Test;

import argo.avro.GroupEndpoint;



public class EndpointGroupManagerV2Test {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
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
	public void test() throws URISyntaxException, IOException {
		// Prepare Resource File
		URL resAvroFile = EndpointGroupManagerV2Test.class.getResource("/avro/group_endpoints_v2.avro");
		File avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManagerV2 ge = new EndpointGroupManagerV2();
		// Test loading file 2
		ge.loadAvro(avroFile);
		
		// Prepare Resource File
		resAvroFile = EndpointGroupManagerV2Test.class.getResource("/avro/gp_day01.avro");
		avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManagerV2 geDay01 = new EndpointGroupManagerV2();
		// Test loading file 2
		geDay01.loadAvro(avroFile);
		// Prepare Resource File
		resAvroFile = EndpointGroupManagerV2Test.class.getResource("/avro/gp_day02.avro");
		avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManagerV2 geDay02 = new EndpointGroupManagerV2();
		// Test loading file 2
		geDay02.loadAvro(avroFile);
		// Prepare Resource File
		resAvroFile = EndpointGroupManagerV2Test.class.getResource("/avro/gp_day03.avro");
		avroFile = new File(resAvroFile.toURI());
		// Instantiate class
		EndpointGroupManagerV2 geDay03 = new EndpointGroupManagerV2();
		// Test loading file 2
		geDay03.loadAvro(avroFile);
		
		
		assertNotNull("File Loaded", ge);
		assertNotNull("File Loaded", geDay01);
		assertNotNull("File Loaded", geDay02);
		assertNotNull("File Loaded", geDay03);
		
		

		// Test Check if service endpoint exists in topology
		assertTrue(ge.checkEndpoint("storage1.grid.upjs.sk", "ARC-CE"));
		assertTrue(ge.checkEndpoint("storage1.grid.upjs.sk", "ARC-CE"));
		assertTrue(ge.checkEndpoint("se01.afroditi.hellasgrid.gr", "SRMv2"));
		assertTrue(ge.checkEndpoint("grid-perfsonar.hpc.susx.ac.uk", "net.perfSONAR.Latency"));
		assertTrue(ge.checkEndpoint("se.grid.tuke.sk", "SRMv2"));
		assertTrue(ge.checkEndpoint("dpm.grid.atomki.hu", "SRMv2"));
		assertTrue(ge.checkEndpoint("gt3.pnpi.nw.ru", "CREAM-CE"));
		
	
		
		// Test check Group retrieval
		ArrayList<String> result1 = new ArrayList<String>();
		result1.add("ru-PNPI");
		assertEquals(ge.getGroup( "gt3.pnpi.nw.ru", "CREAM-CE"), result1);
		

		// Check non-existent groups
		assertTrue(ge.checkEndpoint("ce.etfos.cro-ngi.hr", "GRAM5") == false);
		assertTrue(ge.checkEndpoint("grid129.sinp.msu.ru", "CREAM-CE") == false);
		
		ArrayList<GroupEndpoint> egpList = new ArrayList<GroupEndpoint>();
		for (int i=0; i<5; i++){
			GroupEndpoint itemNew = new GroupEndpoint();
			itemNew.setGroup("FOO");
			itemNew.setHostname("host_"+Integer.toString(i));
			itemNew.setType("SITE");
			egpList.add(itemNew);
		}
		
		EndpointGroupManagerV2 egpMgr = new EndpointGroupManagerV2();
		egpMgr.loadFromList(egpList);
		
		assertTrue(egpMgr.getList().size()==5);
		
		ArrayList<String> toBeRemoved = geDay01.compareToBeRemoved(geDay03);
		assertTrue(toBeRemoved.toString().equals("[SITES,RRC-KI-T1,CREAM-CE,calc1.t1.grid.kiae.ru, SITES,UKI-NORTHGRID-MAN-HEP,CREAM-CE,ce03.tier2.hep.manchester.ac.uk, SITES,INFN-ROMA1,gLExec,atlas-ce-02.roma1.infn.it, SITES,INFN-ROMA1,gLExec,atlas-creamce-02.roma1.infn.it, SITES,UKI-NORTHGRID-LANCS-HEP,com.ceph.object-storage,storage.datacentred.io, SITES,UKI-NORTHGRID-MAN-HEP,gLExec,ce03.tier2.hep.manchester.ac.uk, SITES,INFN-ROMA1,gLExec,atlas-creamce-01.roma1.infn.it, SITES,PSNC,ARC-CE,cream01.egee.man.poznan.pl, SITES,UKI-SCOTGRID-ECDF,gLite-APEL,mon2.glite.ecdf.ed.ac.uk, SITES,UKI-NORTHGRID-MAN-HEP,APEL,ce03.tier2.hep.manchester.ac.uk, SITES,INFN-ROMA1,CREAM-CE,atlas-creamce-01.roma1.infn.it]"));
		
	}

}
