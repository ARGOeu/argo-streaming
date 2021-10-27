package flipflops;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author cthermolia
 */
public class Trends {
    private String group;
    private String service;
    private String endpoint;
    private String metric;
    private int flipflop;

    private String status;
    private int trends;
    private int duration;

    public Trends(String group, String service, String endpoint, String metric, String status, int trends, int duration) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.status = status;
        this.trends = trends;
        this.duration=duration;
    }

    public Trends(String group, String service, String endpoint, String metric, String status, int trends) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.flipflop = flipflop;
        this.status = status;
        this.trends = trends;
    }
    
    

    public Trends(String group, String service, String endpoint, String metric, int flipflop) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.metric = metric;
        this.flipflop = flipflop;
    }

    public Trends(String group, int flipflop) {
        this.group = group;
        this.flipflop = flipflop;
    }

    public Trends(String group, String service, int flipflop) {
        this.group = group;
        this.service = service;
        this.flipflop = flipflop;
    }

    public Trends(String group, String service, String endpoint, int flipflop) {
        this.group = group;
        this.service = service;
        this.endpoint = endpoint;
        this.flipflop = flipflop;
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

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public int getFlipflop() {
        return flipflop;
    }

    public void setFlipflop(int flipflop) {
        this.flipflop = flipflop;
    }
    
    
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getTrends() {
        return trends;
    }

    public void setTrends(int trends) {
        this.trends = trends;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }
    
    
    
}
