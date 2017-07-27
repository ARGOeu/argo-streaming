package argo.batch;

import static org.junit.Assert.*;

import org.junit.Test;

public class MonTimelineTest {

	@Test
	public void test() {
		MonTimeline monA = new MonTimeline();
		assertEquals("timeline size init",1440,monA.getTimeline().length);
		assertEquals("group name init","",monA.getGroup());
		assertEquals("service name init","",monA.getService());
		assertEquals("hostname name init","",monA.getHostname());
		assertEquals("metric name init","",monA.getMetric());
		
		MonTimeline monB = new MonTimeline("fooGroup","fooService","fooHost","fooMetric",1445);
		
		assertEquals("timeline size set",1445,monB.getTimeline().length);
		assertEquals("group name set","fooGroup",monB.getGroup());
		assertEquals("service name set","fooService",monB.getService());
		assertEquals("hostname name set","fooHost",monB.getHostname());
		assertEquals("metric name set","fooMetric",monB.getMetric());
		
		MonTimeline monC = new MonTimeline("fooGroup2","fooService2","fooHost2","fooMetric2");
		
		assertEquals("timeline size set",1440,monC.getTimeline().length);
		assertEquals("group name set","fooGroup2",monC.getGroup());
		assertEquals("service name set","fooService2",monC.getService());
		assertEquals("hostname name set","fooHost2",monC.getHostname());
		assertEquals("metric name set","fooMetric2",monC.getMetric());
		
	}

}
