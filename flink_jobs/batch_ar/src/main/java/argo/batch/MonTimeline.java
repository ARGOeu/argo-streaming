package argo.batch;

import java.util.Arrays;

public class MonTimeline {

	private String group;
	private String service;
	private String hostname;
	private String metric;
	private int[] timeline;

	public MonTimeline() {
		this.group = "";
		this.service = "";
		this.hostname = "";
		this.metric = "";
		this.timeline = new int[1440];
	}

	public MonTimeline(String _group, String _service, String _hostname, String _metric) {
		this.group = _group;
		this.service = _service;
		this.hostname = _hostname;
		this.metric = _metric;
		this.timeline = new int[1440];

	}

	public MonTimeline(String _group, String _service, String _hostname, String _metric, int n) {
		this.group = _group;
		this.service = _service;
		this.hostname = _hostname;
		this.metric = _metric;
		this.timeline = new int[n];
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public int[] getTimeline() {
		return timeline;
	}

	public void setTimeline(int[] timeline) {
		this.timeline = timeline;
	}

	public String toString() {
		return "(" + this.group + "," + this.service + "," + this.hostname + "," + this.metric + "," + Arrays.toString(this.timeline) + ")";
	}

}
