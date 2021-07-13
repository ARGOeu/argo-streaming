package argo.batch;

public class EndpointAR {
	
	private int dateInt;
	private String report;
	private String name;
	private String service;
	private String group;
	private double a;
	private double r;
	private double up;
	private double unknown;
	private double down;
	
	public EndpointAR(int _dateInt, String _report, String _name, String _service, String _group, double _a, double _r, double _up, double _unknown, double _down){
		this.dateInt = _dateInt;
		this.report=_report;
		this.name = _name;
		this.service = _service;
		this.group = _group;
		this.a = _a;
		this.r = _r;
		this.up = _up;
		this.unknown = _unknown;
		this.down = _down;
	}
	
	
	public String getService() {
		return this.service;
	}
	
	public void setService(String service) {
		this.service = service;
	}
	
	public int getDateInt(){
		return this.dateInt;
	}
	
	public void setDateInt(int dateInt){
		this.dateInt= dateInt;
	}
	
	public String getReport() {
		return report;
	}
	public void setReport(String report) {
		this.report = report;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
	public double getA() {
		return a;
	}
	public void setA(double a) {
		this.a = a;
	}
	public double getR() {
		return r;
	}
	public void setR(double r) {
		this.r = r;
	}
	public double getUp() {
		return up;
	}
	public void setUp(double up) {
		this.up = up;
	}
	public double getUnknown() {
		return unknown;
	}
	public void setUnknown(double unknown) {
		this.unknown = unknown;
	}
	public double getDown() {
		return down;
	}
	public void setDown(double down) {
		this.down = down;
	}
	
	public String toString() {
		return "(" + this.dateInt+ "," + this.report + "," + this.name + "," + this.service + "," + this.group + "," + this.a + "," + this.r + ","  + this.up + ","  + this.unknown + ","  + this.down  + ")";
	}

}
