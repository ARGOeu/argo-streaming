package argo.batch;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import java.net.URL;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import org.junit.BeforeClass;
import org.junit.Test;


import argo.avro.MetricData;
import sync.RecomputationManagerTest;

public class ExcludeMetricDataTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Assert that files are present
		assertNotNull("Test file missing", ExcludeMetricDataTest.class.getResource("/ops/recomp.json"));
	
	}

	@Test
	public void test() throws Exception {
		
				// Prepare Resource File which contains recomputations
				URL resJsonFile = RecomputationManagerTest.class.getResource("/ops/recomp.json.flink");
				File jsonFile = new File(resJsonFile.toURI());

				// Prepare a local flink execution environment for testing
				ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();

		        env.setParallelism(1);
		     
		        // Create a data set with metric data - some of them are coming from monitoring host bad-mon.example.org
		        // which should be excluded during 02:00 and 06:00 period of 2019-05-07 
		        MetricData md01 = new MetricData("2019-05-07T00:00:00Z","CREAM-CE","host01.example.org","metric01","CRITICAL","bad-mon01.example.org","","summary","msg",null);
		        MetricData md02 = new MetricData("2019-05-07T01:00:00Z","CREAM-CE","host01.example.org","metric01","OK","good-mon01.example.org","","summary","msg",null);
		        MetricData md03 = new MetricData("2019-05-07T03:00:00Z","CREAM-CE","host01.example.org","metric01","OK","good-mon02.example.org","","summary","msg",null);
		        MetricData md04 = new MetricData("2019-05-07T03:32:00Z","CREAM-CE","host01.example.org","metric01","CRITICAL","bad-mon01.example.org","","summary","msg",null);
		        MetricData md05 = new MetricData("2019-05-07T04:00:00Z","CREAM-CE","host01.example.org","metric01","OK","good-mon01.example.org","","summary","msg",null);
		        MetricData md06 = new MetricData("2019-05-07T04:32:00Z","CREAM-CE","host01.example.org","metric01","CRITICAL","bad-mon01.example.org","","summary","msg",null);
		        MetricData md07 = new MetricData("2019-05-07T05:00:00Z","CREAM-CE","host01.example.org","metric01","OK","good-mon02.example.org","","summary","msg",null);
		        MetricData md08 = new MetricData("2019-05-07T06:00:00Z","CREAM-CE","host01.example.org","metric01","OK","good-mon01.example.org","","summary","msg",null);
		        MetricData md09 = new MetricData("2019-05-07T07:00:00Z","CREAM-CE","host01.example.org","metric01","OK","bad-mon1.example.org","","summary","msg",null);
			       
		        // Create a recomputation dataset by reading the recomputation file. This dataset will
		        // be used as a broadcast variable
		        String recStr = new String();
		        BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		        recStr = br.readLine();
		        br.close();
		        DataSet<String> recDS = env.fromElements(recStr);
		       
		        // Read the initial metric data
		        DataSet<MetricData> md = env.fromElements(md01,md02,md03,md04,md05,md06,md07,md08,md09);
		        // Clean the metric data by testing the ExcludeMetricData flatmap function with 
		        // recomputation information to exclude bad-mon01.example.org data from 02:00 to 06:00 (broadcast variable)
		        DataSet<MetricData> clearMd = md.flatMap(new ExcludeMetricData(null)).withBroadcastSet(recDS, "rec");
		        
		        // collect the final result in a list
		        List<MetricData> resulted = clearMd.collect();
		        
		        // Create the expected result in a list
		        List<MetricData> expected = new ArrayList<MetricData>();
		        expected.add(md01);
		        expected.add(md02);
		        expected.add(md03);
		        expected.add(md05);
		        expected.add(md07);
		        expected.add(md08);
		        expected.add(md09);

		        // compare expected and resulted data
		        assertEquals(expected,resulted);
				
		
	}
	
	
	

}
