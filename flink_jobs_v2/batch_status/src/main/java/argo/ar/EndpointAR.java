package argo.ar;

import java.util.Objects;



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
    private String info;
	
	public EndpointAR(int _dateInt, String _report, String _name, String _service, String _group, double _a, double _r, double _up, double _unknown, double _down, String _info){
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
		this.info = _info;
		
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
	
	public void setInfo(String info) {
		this.info = info;
	}
	
	public String getInfo() {
		return this.info;
	}
	
	public String toString() {
		return "(" + this.dateInt+ "," + this.report + "," + this.name + "," + this.service + "," + this.group + "," + this.a + "," + this.r + ","  + this.up + ","  + this.unknown + ","  + this.down  + ")";
	}

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + this.dateInt;
        hash = 97 * hash + Objects.hashCode(this.report);
        hash = 97 * hash + Objects.hashCode(this.name);
        hash = 97 * hash + Objects.hashCode(this.service);
        hash = 97 * hash + Objects.hashCode(this.group);
        hash = 97 * hash + (int) (Double.doubleToLongBits(this.a) ^ (Double.doubleToLongBits(this.a) >>> 32));
        hash = 97 * hash + (int) (Double.doubleToLongBits(this.r) ^ (Double.doubleToLongBits(this.r) >>> 32));
        hash = 97 * hash + (int) (Double.doubleToLongBits(this.up) ^ (Double.doubleToLongBits(this.up) >>> 32));
        hash = 97 * hash + (int) (Double.doubleToLongBits(this.unknown) ^ (Double.doubleToLongBits(this.unknown) >>> 32));
        hash = 97 * hash + (int) (Double.doubleToLongBits(this.down) ^ (Double.doubleToLongBits(this.down) >>> 32));
        hash = 97 * hash + Objects.hashCode(this.info);
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
        final EndpointAR other = (EndpointAR) obj;
        if (this.dateInt != other.dateInt) {
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
        if (!Objects.equals(this.service, other.service)) {
            return false;
        }
        if (!Objects.equals(this.group, other.group)) {
            return false;
        }
        return Objects.equals(this.info, other.info);
    }

}
