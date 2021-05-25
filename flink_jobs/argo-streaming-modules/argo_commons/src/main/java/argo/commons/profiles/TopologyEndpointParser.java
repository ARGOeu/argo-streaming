package argo.commons.profiles;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//import argo.utils.RequestManager;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import argo.commons.requests.RequestManager;

/**
 *
 * @author cthermolia
 *
 * * TopologyEndpointParser, collects data as described in the json received
 * from web api topology endpoint request
 */
public class TopologyEndpointParser implements Serializable {

    private HashMap<String, ArrayList<EndpointGroup>> topologyEndPointsPerType=new HashMap<>();

    private HashMap<String, HashMap<String, String>> topologyEndpoint=new HashMap<>();
    private final String url = "/topology/endpoints/by_report";
// private final String url = "/topology/endpoints";
    private JSONObject jsonObject;
    public TopologyEndpointParser() {
    }

    public TopologyEndpointParser(String apiUri, String key, String proxy, String date, String reportname) throws IOException, ParseException {
        // by_report/{report-name}?date=YYYY-MM-DD
        String uri = apiUri + url + "/" + reportname;
        // String uri = apiUri + url;

        if (date != null) {
            uri = uri + "?date=" + date;
        }
        loadTopologyEndpoints(uri, key, proxy);
    }

    public TopologyEndpointParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }

    public HashMap<String, String> getTopology(String type) throws IOException, org.json.simple.parser.ParseException {

        return topologyEndpoint.get(type);
    }

    private void loadTopologyEndpoints(String uri, String key, String proxy) throws IOException, ParseException {
      
         jsonObject = RequestManager.request(uri, key, proxy);
    }
     public void readApiRequestResult(){
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

                EndpointGroup endpointGroup = new EndpointGroup(group, hostname, service, type, tag);

                ArrayList<EndpointGroup> topologies = new ArrayList<>();
                if (topologyEndPointsPerType.get(type) != null) {
                    topologies = topologyEndPointsPerType.get(type);
                }
                topologies.add(endpointGroup);

                topologyEndPointsPerType.put(type, topologies);
            }
        }

    }

    public String retrieveGroup(String type, String serviceEndpoint) {
        if (topologyEndpoint.get(type) == null) {
            return null;
        }
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

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 53 * hash + Objects.hashCode(this.group);
            hash = 53 * hash + Objects.hashCode(this.hostname);
            hash = 53 * hash + Objects.hashCode(this.service);
            hash = 53 * hash + Objects.hashCode(this.type);
            hash = 53 * hash + Objects.hashCode(this.tags);
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
            final EndpointGroup other = (EndpointGroup) obj;
            if (!Objects.equals(this.group, other.group)) {
                return false;
            }
            if (!Objects.equals(this.hostname, other.hostname)) {
                 return false;
            }
            if (!Objects.equals(this.service, other.service)) {
                return false;
            }
            if (!Objects.equals(this.type, other.type)) {
               return false;
            }
            if (!Objects.equals(this.tags, other.tags)) {
                 return false;
            }
            return true;
        }

      
      
    }

    public class Tags implements Serializable {

        private String scope;
        private String production;
        private String monitored;

        public Tags() {
        }

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

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 37 * hash + Objects.hashCode(this.scope);
            hash = 37 * hash + Objects.hashCode(this.production);
            hash = 37 * hash + Objects.hashCode(this.monitored);
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
            final Tags other = (Tags) obj;
            if (!Objects.equals(this.scope, other.scope)) {
                System.out.println("this scope ---- "+this.scope+" other scope ----"+other.scope);
                return false;
            }
            if (!Objects.equals(this.production, other.production)) {
                  System.out.println("this production ---- "+this.production+" other production ----"+other.production);
                return false;
            }
            if (!Objects.equals(this.monitored, other.monitored)) {
                  System.out.println("this monitored ---- "+this.monitored+" other monitored ----"+other.monitored);
                return false;
            }
            return true;
        }

    }

    
 

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.topologyEndPointsPerType);
        hash = 29 * hash + Objects.hashCode(this.topologyEndpoint);
        hash = 29 * hash + Objects.hashCode(this.url);
        hash = 29 * hash + Objects.hashCode(this.jsonObject);
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
        final TopologyEndpointParser other = (TopologyEndpointParser) obj;
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        if (!Objects.equals(this.topologyEndPointsPerType, other.topologyEndPointsPerType)) {
            return false;
        }
        if (!Objects.equals(this.topologyEndpoint, other.topologyEndpoint)) {
            return false;
        }
        if (!Objects.equals(this.jsonObject, other.jsonObject)) {
            return false;
        }
        return true;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }
    
    

}
