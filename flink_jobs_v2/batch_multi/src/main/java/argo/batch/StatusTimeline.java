/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.batch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;

/**
 *
 * StatusMetricTimeline
 */
public class StatusTimeline implements Serializable {

    private String group;
    private String function;
    private String service;
    private String hostname;
    private String metric;
    ArrayList<TimeStatus> timestamps;
    private boolean hasThr;

    public StatusTimeline() {
        group = "";
        function = "";
        service = "";
        hostname = "";
        timestamps = new ArrayList<>();
        hasThr = false;
    }

    public StatusTimeline(String group, String function, String service, String hostname, String metric, ArrayList<TimeStatus> timestamps) {
        this.group = group;
        this.function = function;
        this.service = service;
        this.hostname = hostname;
        this.metric = metric;
        this.timestamps = timestamps;
        hasThr = false;
    }

    public boolean hasThr() {
        return hasThr;
    }

    public void setHasThr(boolean hasThr) {
        this.hasThr = hasThr;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
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

    public ArrayList<TimeStatus> getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(ArrayList<TimeStatus> timestamps) {
        this.timestamps = timestamps;
    }

    @Override
    public String toString() {

        String resultsStatusMetrics = "+";
        String resultsTimeStatus = "+";
        for (TimeStatus st : timestamps) {
            resultsTimeStatus += st.toString();
        }
        return "StatusTimeline{" + "group=" + group + ", service=" + service + ", hostname=" + hostname + ", metric=" + metric + ", statusMetrics=" + resultsStatusMetrics + ", timestamps=" + resultsTimeStatus + '}';
    }
}
