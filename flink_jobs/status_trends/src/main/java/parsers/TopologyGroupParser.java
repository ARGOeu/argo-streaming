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
public class TopologyGroupParser {

    private HashMap<String, ArrayList<TopologyGroup>> topologyGroups = new HashMap<>();

    public TopologyGroupParser(String baseUri, String key, String proxy,String date) throws IOException, ParseException {
        loadTopologyGroups(baseUri, key, proxy,date);
    
    }

    
    
    public void loadTopologyGroups(String baseUri, String key, String proxy,String date) throws IOException, ParseException {

        JSONObject jsonObject = RequestManager.getTopologyGroupRequest(baseUri, key, proxy,date);
        HashMap<String, String> jsonDataMap = new HashMap<>();

        JSONArray data = (JSONArray) jsonObject.get("data");

        Iterator<Object> dataIter = data.iterator();
        while (dataIter.hasNext()) {
            Object dataobj = dataIter.next();
            if (dataobj instanceof JSONObject) {
                JSONObject jsonDataObj = new JSONObject((Map) dataobj);
                String group = (String) jsonDataObj.get("group");
                String type = (String) jsonDataObj.get("type");
                String subgroup = (String) jsonDataObj.get("subgroup");
                JSONObject tagsObj = (JSONObject) jsonDataObj.get("tags");
                Tags tag = null;
                if (tagsObj != null) {
                    String scope = (String) tagsObj.get("scope");
                    String production = (String) tagsObj.get("production");
                    String monitored = (String) tagsObj.get("monitored");

                    tag = new Tags(scope, production, monitored);
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
                if (topologyGroups.get(type) != null) {
                    groupList = topologyGroups.get(type);
                }
                groupList.add(topologyGroup);

            }

        }
    }

    public HashMap<String, ArrayList<TopologyGroup>> getTopologyGroups() {
        return topologyGroups;
    }

    public class TopologyGroup {

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

    }

    public class Tags {

        private String scope;
        private String infrastructure;
        private String certification;

        public Tags(String scope, String infrastructure, String certification) {
            this.scope = scope;
            this.infrastructure = infrastructure;
            this.certification = certification;
        }

        public String getScope() {
            return scope;
        }

        public String getInfrastructure() {
            return infrastructure;
        }

        public String getCertification() {
            return certification;
        }

    }

    public class Notifications {

        private String contacts;
        private String enabled;

        public Notifications(String contacts, String enabled) {
            this.contacts = contacts;
            this.enabled = enabled;
        }

    }

}
