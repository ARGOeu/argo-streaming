/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parsers;

import argo.utils.RequestManager;
import java.io.IOException;
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
 */
public class TopologyEndpointParser {

    private HashMap<String, ArrayList<EndpointGroup>> endpointGroups;

    private HashMap<String, String> topologyEndpoint;

    public TopologyEndpointParser(String baseUri, String key, String proxy,String date) throws IOException, ParseException {
        loadTopologyEndpoints(baseUri, key, proxy,date);
    }

    public static HashMap<String, String> getEndpoints(ArrayList<TopologyEndpointParser.EndpointGroup> endpointList) throws IOException, org.json.simple.parser.ParseException {

        HashMap<String, String> jsonDataMap = new HashMap<>();

        Iterator<TopologyEndpointParser.EndpointGroup> dataIter = endpointList.iterator();
        while (dataIter.hasNext()) {
            TopologyEndpointParser.EndpointGroup dataobj = dataIter.next();

            String hostname = dataobj.getHostname();
            String service = dataobj.getService();
            String group = dataobj.getGroup();
            jsonDataMap.put(hostname + "-" + service, group);

        }

        return jsonDataMap;
    }

    private void loadTopologyEndpoints(String baseUri, String key, String proxy,String date) throws IOException, ParseException {
        endpointGroups=new HashMap<>();
        topologyEndpoint = new HashMap<>();
        JSONObject jsonObject = RequestManager.getTopologyEndpointRequest(baseUri, key, proxy,date);
        HashMap<String, String> jsonDataMap = new HashMap<>();

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
                topologyEndpoint.put(topologyEndpointKey, group);

                EndpointGroup endpointGroup = new EndpointGroup(group, hostname, service, key, tag);

                ArrayList<EndpointGroup> topologies = new ArrayList<>();
                if (endpointGroups.get(type) != null) {
                    topologies = endpointGroups.get(type);
                }
                topologies.add(endpointGroup);
            }
        }

    }

    public HashMap<String, String> getTopologyEndpoint() {
        return topologyEndpoint;
    }
    

    public HashMap<String, ArrayList<EndpointGroup>> getEndpointGroups() {
        return endpointGroups;
    }

    public class EndpointGroup {

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
