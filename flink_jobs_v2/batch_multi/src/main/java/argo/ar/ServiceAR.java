package argo.ar;

import java.util.Objects;

public class ServiceAR {

    private int dateInt;
    private String report;
    private String name;
    private String group;
    private double a;
    private double r;
    private double up;
    private double unknown;
    private double down;
    
    public ServiceAR(int _dateInt, String _report, String _name, String _group, double _a, double _r, double _up, double _unknown, double _down) {
        this.dateInt = _dateInt;
        this.report = _report;
        this.name = _name;
        this.group = _group;
        this.a = _a;
        this.r = _r;
        this.up = _up;
        this.unknown = _unknown;
        this.down = _down;
     
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

   

    public int getDateInt() {
        return this.dateInt;
    }

    public void setDateInt(int dateInt) {
        this.dateInt = dateInt;
    }

    public String getReport() {
        return report;
    }

    public void setReport(String report) {
        this.report = report;
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
        return "(" + this.dateInt + "," + this.report  + "," + this.name + "," + this.group + "," + this.a + "," + this.r + "," + this.up + "," + this.unknown + "," + this.down + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ServiceAR)) return false;
        ServiceAR serviceAR = (ServiceAR) o;
        return dateInt == serviceAR.dateInt && Double.compare(a, serviceAR.a) == 0 && Double.compare(r, serviceAR.r) == 0 && Double.compare(up, serviceAR.up) == 0 && Double.compare(unknown, serviceAR.unknown) == 0 && Double.compare(down, serviceAR.down) == 0 && Objects.equals(report, serviceAR.report) && Objects.equals(name, serviceAR.name) && Objects.equals(group, serviceAR.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dateInt, report, name, group, a, r, up, unknown, down);
    }
}
