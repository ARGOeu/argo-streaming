/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.profiles;

import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 *
 * * TopologyEndpointParser, collects data as described in the json received
 * from web api topology endpoint request
 */
public class TopologyEndpointParser implements Serializable {

    private HashMap<String, ArrayList<EndpointGroup>> topologyEndPointsPerType;

    private HashMap<String, HashMap<String, String>> topologyEndpoint;
    private final String url = "/topology/endpoints/by_report";
// private final String url = "/topology/endpoints";

    public TopologyEndpointParser() {
    }

    public TopologyEndpointParser(String apiUri, String key, String proxy, String date, String reportname) throws IOException, ParseException {
        // by_report/{report-name}?date=YYYY-MM-DD
        String uri = apiUri + url + "/" + reportname;

        if (date != null) {
            uri = uri + "?date=" + date;
        }
        loadTopologyEndpoints(uri, key, proxy);
    }

    public HashMap<String, String> getTopology(String type) throws IOException, org.json.simple.parser.ParseException {

        return topologyEndpoint.get(type);
    }

    private void loadTopologyEndpoints(String uri, String key, String proxy) throws IOException, ParseException {
        topologyEndPointsPerType = new HashMap<>();
        topologyEndpoint = new HashMap<>();
        JSONObject jsonObject = RequestManager.request(uri, key, proxy);

        JSONArray data = (JSONArray) jsonObject.get("data");

        Iterator<Object> dataIter = data.iterator();
        while (dataIter.hasNext()) {
            Object dataobj = dataIter.next();
            if (dataobj instanceof JSONObject) {
                JSONObject jsonDataObj = new JSONObject((Map) dataobj);
                String hostname = (String) jsonDataObj.get("hostname");
                String service = (String) jsonDataObj.get("service");
                String group = (String) jsonDataObj.get("group");
                String type = (String) jsonDataObj.get("type");
                JSONObject tagsObj = (JSONObject) jsonDataObj.get("tags");

                String scope = (String) tagsObj.get("scope");
                String production = (String) tagsObj.get("production");
                String monitored = (String) tagsObj.get("monitored");
                Tags tag = new Tags(scope, production, monitored);

                String topologyEndpointKey = hostname + "-" + service;

                HashMap<String, String> endpMap = new HashMap<String, String>();
                if (topologyEndpoint.get(type) != null) {
                    endpMap = topologyEndpoint.get(type);
                }

                endpMap.put(topologyEndpointKey, group);
                topologyEndpoint.put(type, endpMap);

                EndpointGroup endpointGroup = new EndpointGroup(group, hostname, service, key, tag);

                ArrayList<EndpointGroup> topologies = new ArrayList<>();
                if (topologyEndPointsPerType.get(type) != null) {
                    topologies = topologyEndPointsPerType.get(type);
                }
                topologies.add(endpointGroup);
            }
        }

    }

    public String retrieveGroup(String type, String serviceEndpoint) {
        return topologyEndpoint.get(type).get(serviceEndpoint);

    }

    public HashMap<String, ArrayList<EndpointGroup>> getTopologyEndPointsPerType() {
        return topologyEndPointsPerType;
    }

    public void setTopologyEndPointsPerType(HashMap<String, ArrayList<EndpointGroup>> topologyEndPointsPerType) {
        this.topologyEndPointsPerType = topologyEndPointsPerType;
    }

    public HashMap<String, HashMap<String, String>> getTopologyEndpoint() {
        return topologyEndpoint;
    }

    public void setTopologyEndpoint(HashMap<String, HashMap<String, String>> topologyEndpoint) {
        this.topologyEndpoint = topologyEndpoint;
    }

    public class EndpointGroup implements Serializable {

        private String group;

        private String hostname;
        private String service;
        private String type;
        private Tags tags;

        public EndpointGroup(String group, String hostname, String service, String type, Tags tags) {
            this.group = group;
            this.hostname = hostname;
            this.service = service;
            this.type = type;
            this.tags = tags;
        }

        public String getGroup() {
            return group;
        }

        public String getHostname() {
            return hostname;
        }

        public String getService() {
            return service;
        }

        public String getType() {
            return type;
        }

        public Tags getTags() {
            return tags;
        }

    }

    public class Tags {

        private String scope;
        private String production;
        private String monitored;

        public Tags(String scope, String production, String monitored) {
            this.scope = scope;
            this.production = production;
            this.monitored = monitored;
        }

        public String getScope() {
            return scope;
        }

        public String getProduction() {
            return production;
        }

        public String getMonitored() {
            return monitored;
        }

    }

}
