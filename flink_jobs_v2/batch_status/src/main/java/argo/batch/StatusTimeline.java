/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.batch;

import java.io.Serializable;
import java.util.ArrayList;

/**
 *
 * StatusMetricTimeline
 */
public class StatusTimeline implements Serializable {

    private String group;
    private String service;
    private String hostname;
    private String metric;
    private ArrayList<StatusMetric> statusMetrics;
    ArrayList<TimeStatus> timestamps;

    public StatusTimeline() {
        group = "";
        service = "";
        hostname = "";
        statusMetrics = new ArrayList<>();
        timestamps = new ArrayList<>();
    }

    public StatusTimeline(String group, String service, String hostname, String metric, ArrayList<StatusMetric> statusMetrics, ArrayList<TimeStatus> timestamps) {
        this.group = group;
        this.service = service;
        this.hostname = hostname;
        this.metric = metric;
        this.statusMetrics = statusMetrics;
        this.timestamps = timestamps;
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

    public ArrayList<StatusMetric> getStatusMetrics() {
        return statusMetrics;
    }

    public void setStatusMetrics(ArrayList<StatusMetric> statusMetrics) {
        this.statusMetrics = statusMetrics;
    }

    public ArrayList<TimeStatus> getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(ArrayList<TimeStatus> timestamps) {
        this.timestamps = timestamps;
    }

    @Override
    public String toString() {

        String resultsStatusMetrics = "+";
        for (StatusMetric st : statusMetrics) {
            resultsStatusMetrics += st.toString();
        }
        String resultsTimeStatus = "+";
        for (TimeStatus st : timestamps) {
            resultsTimeStatus += st.toString();
        }
        return "StatusTimeline{" + "group=" + group + ", service=" + service + ", hostname=" + hostname + ", metric=" + metric + ", statusMetrics=" + resultsStatusMetrics + ", timestamps=" + resultsTimeStatus + '}';
    }

}
