package argo.ar;

import java.util.Objects;





public class EndpointGroupAR {
	
	private int dateInt;
	private String report;
	private String name;
	private String group;
	private int weight;
	private double a;
	private double r;
	private double up;
	private double unknown;
	private double down;
	
	public EndpointGroupAR(int _dateInt, String _report, String _name, String _group, int _weight, double _a, double _r, double _up, double _unknown, double _down){
		this.dateInt = _dateInt;
		this.report=_report;
		this.name = _name;
		this.group = _group;
		this.weight = _weight;
		this.a = _a;
		this.r = _r;
		this.up = _up;
		this.unknown = _unknown;
		this.down = _down;
	}
	
	public int getWeight(){
		return this.weight;
	}
	
	public void setWeight(int weight){
		this.weight = weight;
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
		return "(" + this.dateInt+ "," + this.report + "," + this.name + "," + this.group + "," + this.weight + "," + this.a + "," + this.r + ","  + this.up + ","  + this.unknown + ","  + this.down  + ")";
	}

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + this.dateInt;
        hash = 59 * hash + Objects.hashCode(this.report);
        hash = 59 * hash + Objects.hashCode(this.name);
        hash = 59 * hash + Objects.hashCode(this.group);
        hash = 59 * hash + this.weight;
        hash = 59 * hash + (int) (Double.doubleToLongBits(this.a) ^ (Double.doubleToLongBits(this.a) >>> 32));
        hash = 59 * hash + (int) (Double.doubleToLongBits(this.r) ^ (Double.doubleToLongBits(this.r) >>> 32));
        hash = 59 * hash + (int) (Double.doubleToLongBits(this.up) ^ (Double.doubleToLongBits(this.up) >>> 32));
        hash = 59 * hash + (int) (Double.doubleToLongBits(this.unknown) ^ (Double.doubleToLongBits(this.unknown) >>> 32));
        hash = 59 * hash + (int) (Double.doubleToLongBits(this.down) ^ (Double.doubleToLongBits(this.down) >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final EndpointGroupAR other = (EndpointGroupAR) obj;
        if (this.dateInt != other.dateInt) {
            return false;
        }
        if (this.weight != other.weight) {
            return false;
        }
        if (Double.doubleToLongBits(this.a) != Double.doubleToLongBits(other.a)) {
            return false;
        }
        if (Double.doubleToLongBits(this.r) != Double.doubleToLongBits(other.r)) {
            return false;
        }
        if (Double.doubleToLongBits(this.up) != Double.doubleToLongBits(other.up)) {
            return false;
        }
        if (Double.doubleToLongBits(this.unknown) != Double.doubleToLongBits(other.unknown)) {
            return false;
        }
        if (Double.doubleToLongBits(this.down) != Double.doubleToLongBits(other.down)) {
            return false;
        }
        if (!Objects.equals(this.report, other.report)) {
            return false;
        }
        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        return Objects.equals(this.group, other.group);
    }
        
        

}
