/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parsers;

import argo.utils.RequestManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class ReportParser {

    public static TenantReport tenantReport;

    public static void loadReportInfo(String baseUri, String key, String proxy, String reportId) throws IOException, ParseException {

        JSONObject jsonObject = RequestManager.getReportRequest(baseUri, key, proxy, reportId);

        JSONArray dataList = (JSONArray) jsonObject.get("data");

        Iterator<JSONObject> iterator = dataList.iterator();

        while (iterator.hasNext()) {
            JSONObject dataObject = (JSONObject) iterator.next();

            String id = (String) dataObject.get("id");
            String tenant = (String) dataObject.get("tenant");
            boolean disabled = (boolean) dataObject.get("disabled");

            JSONObject infoObject = (JSONObject) dataObject.get("info");
            String[] info = new String[4];
            info[0] = (String) infoObject.get("name");
            info[1] = (String) infoObject.get("description");
            info[2] = (String) infoObject.get("created");
            info[3] = (String) infoObject.get("updated");

            JSONObject topologyObject = (JSONObject) dataObject.get("topology_schema");
            JSONObject groupObject = (JSONObject) topologyObject.get("group");

            String type = (String) groupObject.get("type");
            JSONObject subGroupObject = (JSONObject) groupObject.get("group");

            String grouptype = (String) subGroupObject.get("type");

            TopologyGroup group = new TopologyGroup(grouptype,null);
            TopologyGroup topologyGroup = new TopologyGroup(type, group);

            JSONObject thresholdsObject = (JSONObject) dataObject.get("thresholds");

            Threshold threshold = new Threshold((Long) thresholdsObject.get("availability"), (Long) thresholdsObject.get("reliability"), (Double) thresholdsObject.get("uptime"),
                    (Double) thresholdsObject.get("unknown"), (Double) thresholdsObject.get("downtime"));

            JSONArray profiles = (JSONArray) dataObject.get("profiles");

            Iterator<JSONObject> profileIter = profiles.iterator();
            ArrayList<Profiles> profileList = new ArrayList<>();
            while (profileIter.hasNext()) {
                JSONObject profileObject = (JSONObject) profileIter.next();
                Profiles profile = new Profiles((String) profileObject.get("id"), (String) profileObject.get("name"), (String) profileObject.get("type"));
                profileList.add(profile);
            }

            JSONArray filters = (JSONArray) dataObject.get("filter_tags");
            Iterator<JSONObject> filterIter = filters.iterator();
            ArrayList<FilterTags> filtersList = new ArrayList<>();
            while (filterIter.hasNext()) {
                JSONObject filterObject = (JSONObject) filterIter.next();
                FilterTags filter = new FilterTags((String) filterObject.get("name"), (String) filterObject.get("value"), (String) filterObject.get("context"));
                filtersList.add(filter);
            }

            tenantReport = new TenantReport(id, tenant, disabled, info, group, threshold, profileList, filtersList);
        }

    }

    public static class Threshold {

        public Long availability;
        public Long reliability;
        public Double uptime;
        public Double unknown;
        public Double downtime;

        public Threshold(Long availability, Long reliability, Double uptime, Double unknown, Double downtime) {
            this.availability = availability;
            this.reliability = reliability;
            this.uptime = uptime;
            this.unknown = unknown;
            this.downtime = downtime;
        }

    }

    public static class Profiles {

        public String id;
        public String name;
        public String type;

        public Profiles(String id, String name, String type) {
            this.id = id;
            this.name = name;
            this.type = type;
        }
    }

    public static class FilterTags {

        public String name;
        public String value;
        public String context;

        public FilterTags(String name, String value, String context) {
            this.name = name;
            this.value = value;
            this.context = context;
        }

    }

    public static class TopologyGroup {

        public String type;
        public TopologyGroup group;

        public TopologyGroup(String type, TopologyGroup group) {
            this.type = type;
            this.group = group;
        }
    }

    public static class TenantReport {
        public  String id;
        public   String tenant;
        public  boolean disabled;
        public  String[] info;
        public  TopologyGroup group;
        public  Threshold threshold;
        public  ArrayList<Profiles> profiles;
        public  ArrayList<FilterTags> filterTags;

        public TenantReport(String id, String tenant, boolean disabled, String[] info, TopologyGroup group, Threshold threshold, ArrayList<Profiles> profiles, ArrayList<FilterTags> filterTags) {
            this.id = id;
            this.tenant = tenant;
            this.disabled = disabled;
            this.info = info;
            this.group = group;
            this.threshold = threshold;
            this.profiles = profiles;
            this.filterTags = filterTags;
        }

    }
}
