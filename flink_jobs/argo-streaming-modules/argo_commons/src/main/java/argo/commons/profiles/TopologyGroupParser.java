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
 * TopologyGroupParser, collects data as described in the json received from web
 * api topology group request
 */
public class TopologyGroupParser implements Serializable {

    private HashMap<String, ArrayList<TopologyGroup>> topologyGroupsPerType = new HashMap<>();
    private ArrayList<String> topologyGroups = new ArrayList<>();
    private final String url = "/topology/groups/by_report";
    //private final String url = "/topology/groups";
    private JSONObject jsonObject;

    public TopologyGroupParser() {
    }

    public TopologyGroupParser(String apiUri, String key, String proxy, String date, String reportname) throws IOException, ParseException {
        String uri = apiUri + url + "/" + reportname;
        // String uri = apiUri + url;
        if (date != null) {
            uri = uri + "?date=" + date;
        }
        loadTopologyGroups(uri, key, proxy);

    }

    public TopologyGroupParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }
    

    public void loadTopologyGroups(String uri, String key, String proxy) throws IOException, ParseException {

         jsonObject = RequestManager.request(uri, key, proxy);
         readApiRequestResult();

    }

    public void readApiRequestResult() {
        JSONArray data = (JSONArray) jsonObject.get("data");

        Iterator<Object> dataIter = data.iterator();
        while (dataIter.hasNext()) {
            Object dataobj = dataIter.next();
            if (dataobj instanceof JSONObject) {
                JSONObject jsonDataObj = new JSONObject((Map) dataobj);
                String group = (String) jsonDataObj.get("group");
                String type = (String) jsonDataObj.get("type");
                String subgroup = (String) jsonDataObj.get("subgroup");
                topologyGroups.add(subgroup);
                JSONObject tagsObj = (JSONObject) jsonDataObj.get("tags");
                Tags tag = null;
                if (tagsObj != null) {
                    String scope = (String) tagsObj.get("scope");
                    String production = (String) tagsObj.get("production");
                    String monitored = (String) tagsObj.get("monitored");

                    tag = new Tags(scope, monitored, production);
                }
                Notifications notification = null;
                JSONObject notificationsObj = (JSONObject) jsonDataObj.get("notifications");
                if (notificationsObj != null) {
                    String contacts = (String) notificationsObj.get("contacts");
                    String enabled = (String) notificationsObj.get("enabled");
                    notification = new Notifications(contacts, enabled);

                }

                TopologyGroup topologyGroup = new TopologyGroup(group, type, subgroup, tag, notification);
                ArrayList<TopologyGroup> groupList = new ArrayList<>();
                if (topologyGroupsPerType.get(type) != null) {
                    groupList = topologyGroupsPerType.get(type);
                }
                groupList.add(topologyGroup);
                topologyGroupsPerType.put(type, groupList);

            }

        }
    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public boolean containsGroup(String group) {
        if (topologyGroups.contains(group)) {
            return true;
        }
        return false;
    }

    public HashMap<String, ArrayList<TopologyGroup>> getTopologyGroupsPerType() {
        return topologyGroupsPerType;
    }

    public void setTopologyGroupsPerType(HashMap<String, ArrayList<TopologyGroup>> topologyGroupsPerType) {
        this.topologyGroupsPerType = topologyGroupsPerType;
    }

    public ArrayList<String> getTopologyGroups() {
        return topologyGroups;
    }

    public void setTopologyGroups(ArrayList<String> topologyGroups) {
        this.topologyGroups = topologyGroups;
    }

    public class TopologyGroup implements Serializable {

        private String group;
        private String type;
        private String subgroup;

        private Tags tags;
        private Notifications notifications;

        public TopologyGroup(String group, String type, String subgroup, Tags tags, Notifications notifications) {
            this.group = group;
            this.type = type;
            this.subgroup = subgroup;
            this.tags = tags;
            this.notifications = notifications;
        }

        public String getGroup() {
            return group;
        }

        public String getType() {
            return type;
        }

        public String getSubgroup() {
            return subgroup;
        }

        public Tags getTags() {
            return tags;
        }

        public Notifications getNotifications() {
            return notifications;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 53 * hash + Objects.hashCode(this.group);
            hash = 53 * hash + Objects.hashCode(this.type);
            hash = 53 * hash + Objects.hashCode(this.subgroup);
            hash = 53 * hash + Objects.hashCode(this.tags);
            hash = 53 * hash + Objects.hashCode(this.notifications);
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
            final TopologyGroup other = (TopologyGroup) obj;
            if (!Objects.equals(this.group, other.group)) {
                return false;
            }
            if (!Objects.equals(this.type, other.type)) {
                return false;
            }
            if (!Objects.equals(this.subgroup, other.subgroup)) {
                return false;
            }
            if (!Objects.equals(this.tags, other.tags)) {
                return false;
            }
            if (!Objects.equals(this.notifications, other.notifications)) {
                return false;
            }
            return true;
        }

    }

    public class Tags implements Serializable {

        private String scope;
        private String monitored;
        private String production;

        public Tags(String scope, String monitored, String production) {
            this.scope = scope;
            this.monitored = monitored;
            this.production = production;
        }

        public String getScope() {
            return scope;
        }

        public String getMonitored() {
            return monitored;
        }

        public void setMonitored(String monitored) {
            this.monitored = monitored;
        }

        public String getProduction() {
            return production;
        }

        public void setProduction(String production) {
            this.production = production;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 17 * hash + Objects.hashCode(this.scope);
            hash = 17 * hash + Objects.hashCode(this.monitored);
            hash = 17 * hash + Objects.hashCode(this.production);
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
                return false;
            }
            if (!Objects.equals(this.monitored, other.monitored)) {
                return false;
            }
            if (!Objects.equals(this.production, other.production)) {
                return false;
            }
            return true;
        }

    }

    public class Notifications implements Serializable {

        private String contacts;
        private String enabled;

        public Notifications(String contacts, String enabled) {
            this.contacts = contacts;
            this.enabled = enabled;
        }

        public String getContacts() {
            return contacts;
        }

        public void setContacts(String contacts) {
            this.contacts = contacts;
        }

        public String getEnabled() {
            return enabled;
        }

        public void setEnabled(String enabled) {
            this.enabled = enabled;
        }

    }

    public String getUrl() {
        return url;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(this.topologyGroupsPerType);
        hash = 59 * hash + Objects.hashCode(this.topologyGroups);
        hash = 59 * hash + Objects.hashCode(this.url);
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
        final TopologyGroupParser other = (TopologyGroupParser) obj;
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        if (!Objects.equals(this.topologyGroupsPerType, other.topologyGroupsPerType)) {
            return false;
        }
        if (!Objects.equals(this.topologyGroups, other.topologyGroups)) {
            return false;
        }
        return true;
    }

}
