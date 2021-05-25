package argo.commons.profiles;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
//import argo.utils.RequestManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import argo.commons.requests.RequestManager;

/**
 *
 * @author cthermolia
 *
 * ReportParser, collects data as described in the json received from web api
 * report request, that corresponds to the specific tenant
 */
public class ReportParser {

    private TenantReport tenantReport;
    private final String url = "/reports/";
    private JSONObject jsonObject;

    public ReportParser() {
    }

    public ReportParser(String apiUri, String key, String proxy, String reportId) throws IOException, ParseException {
        String uri = apiUri + url + reportId;
        loadReportInfo(uri, key, proxy);
    }

    public ReportParser(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }
    

    public void loadReportInfo(String uri, String key, String proxy) throws IOException, ParseException {

         jsonObject = RequestManager.request(uri, key, proxy);
         readApiRequestResult();
    }

    public void readApiRequestResult() {
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

            Topology group = new Topology(grouptype, null);
            Topology topologyGroup = new Topology(type, group);

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

            tenantReport = new TenantReport(id, tenant, disabled, info, topologyGroup, threshold, profileList, filtersList);
        }

    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }
    
    

//    public String getProfileId(String profileName) {
//        ArrayList<Profiles> profiles = tenantReport.getProfiles();
//        if (profiles != null) {
//            for (Profiles profile : profiles) {
//                if (profile.getType().equalsIgnoreCase(profileName)) {
//                    return profile.id;
//                }
//            }
//        }
//        return null;
//    }
    public String getAggregationReportId() {
        ArrayList<Profiles> profiles = tenantReport.getProfiles();
        if (profiles != null) {
            for (Profiles profile : profiles) {
                if (profile.getType().equalsIgnoreCase(ReportParser.ProfileType.AGGREGATION.name())) {
                    return profile.id;
                }
            }
        }
        return null;
    }

    public String getMetricReportId() {
        ArrayList<Profiles> profiles = tenantReport.getProfiles();
        if (profiles != null) {
            for (Profiles profile : profiles) {
                if (profile.getType().equalsIgnoreCase(ReportParser.ProfileType.METRIC.name())) {
                    return profile.id;
                }
            }
        }
        return null;
    }

    public String getOperationReportId() {
        ArrayList<Profiles> profiles = tenantReport.getProfiles();
        if (profiles != null) {
            for (Profiles profile : profiles) {
                if (profile.getType().equalsIgnoreCase(ReportParser.ProfileType.OPERATIONS.name())) {
                    return profile.id;
                }
            }
        }
        return null;
    }

    public TenantReport getTenantReport() {
        return tenantReport;
    }

    public class Threshold {

        private Long availability;
        private Long reliability;
        private Double uptime;
        private Double unknown;
        private Double downtime;

        public Threshold(Long availability, Long reliability, Double uptime, Double unknown, Double downtime) {
            this.availability = availability;
            this.reliability = reliability;
            this.uptime = uptime;
            this.unknown = unknown;
            this.downtime = downtime;
        }

        public Long getAvailability() {
            return availability;
        }

        public Long getReliability() {
            return reliability;
        }

        public Double getUptime() {
            return uptime;
        }

        public Double getUnknown() {
            return unknown;
        }

        public Double getDowntime() {
            return downtime;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 19 * hash + Objects.hashCode(this.availability);
            hash = 19 * hash + Objects.hashCode(this.reliability);
            hash = 19 * hash + Objects.hashCode(this.uptime);
            hash = 19 * hash + Objects.hashCode(this.unknown);
            hash = 19 * hash + Objects.hashCode(this.downtime);
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
            final Threshold other = (Threshold) obj;
            if (!Objects.equals(this.availability, other.availability)) {
                return false;
            }
            if (!Objects.equals(this.reliability, other.reliability)) {
                return false;
            }
            if (!Objects.equals(this.uptime, other.uptime)) {
                return false;
            }
            if (!Objects.equals(this.unknown, other.unknown)) {
                return false;
            }
            if (!Objects.equals(this.downtime, other.downtime)) {
                return false;
            }
            return true;
        }

    }

    public class Profiles {

        private String id;
        private String name;
        private String type;

        public Profiles(String id, String name, String type) {
            this.id = id;
            this.name = name;
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 59 * hash + Objects.hashCode(this.id);
            hash = 59 * hash + Objects.hashCode(this.name);
            hash = 59 * hash + Objects.hashCode(this.type);
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
            final Profiles other = (Profiles) obj;
            if (!Objects.equals(this.id, other.id)) {
                return false;
            }
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            if (!Objects.equals(this.type, other.type)) {
                return false;
            }
            return true;
        }

    }

    public class FilterTags {

        private String name;
        private String value;
        private String context;

        public FilterTags(String name, String value, String context) {
            this.name = name;
            this.value = value;
            this.context = context;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }

        public String getContext() {
            return context;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 79 * hash + Objects.hashCode(this.name);
            hash = 79 * hash + Objects.hashCode(this.value);
            hash = 79 * hash + Objects.hashCode(this.context);
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
            final FilterTags other = (FilterTags) obj;
            if (!Objects.equals(this.name, other.name)) {
                return false;
            }
            if (!Objects.equals(this.value, other.value)) {
                return false;
            }
            if (!Objects.equals(this.context, other.context)) {
                return false;
            }
            return true;
        }

    }

    public class Topology {

        private String type;
        private Topology group;

        public Topology(String type, Topology group) {
            this.type = type;
            this.group = group;
        }

        public String getType() {
            return type;
        }

        public Topology getGroup() {
            return group;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 79 * hash + Objects.hashCode(this.type);
            hash = 79 * hash + Objects.hashCode(this.group);
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
            final Topology other = (Topology) obj;
            if (!Objects.equals(this.type, other.type)) {
                return false;
            }
            if (!Objects.equals(this.group, other.group)) {
                return false;
            }
            return true;
        }

    }

    public class TenantReport {

        private String id;
        private String tenant;
        private boolean disabled;
        private String[] info;
        private Topology group;
        private Threshold threshold;
        private ArrayList<Profiles> profiles;
        private ArrayList<FilterTags> filterTags;

        public TenantReport(String id, String tenant, boolean disabled, String[] info, Topology group, Threshold threshold, ArrayList<Profiles> profiles, ArrayList<FilterTags> filterTags) {
            this.id = id;
            this.tenant = tenant;
            this.disabled = disabled;
            this.info = info;
            this.group = group;
            this.threshold = threshold;
            this.profiles = profiles;
            this.filterTags = filterTags;

        }

        public String getId() {
            return id;
        }

        public String getTenant() {
            return tenant;
        }

        public boolean isDisabled() {
            return disabled;
        }

        public String[] getInfo() {
            return info;
        }

        public Topology getGroup() {
            return group;
        }

        public Threshold getThreshold() {
            return threshold;
        }

        public ArrayList<Profiles> getProfiles() {
            return profiles;
        }

        public ArrayList<FilterTags> getFilterTags() {
            return filterTags;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 37 * hash + Objects.hashCode(this.id);
            hash = 37 * hash + Objects.hashCode(this.tenant);
            hash = 37 * hash + (this.disabled ? 1 : 0);
            hash = 37 * hash + Arrays.deepHashCode(this.info);
            hash = 37 * hash + Objects.hashCode(this.group);
            hash = 37 * hash + Objects.hashCode(this.threshold);
            hash = 37 * hash + Objects.hashCode(this.profiles);
            hash = 37 * hash + Objects.hashCode(this.filterTags);
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
            final TenantReport other = (TenantReport) obj;
            if (this.disabled != other.disabled) {
                return false;
            }
            if (!Objects.equals(this.id, other.id)) {
                 return false;
            }
            if (!Objects.equals(this.tenant, other.tenant)) {
                return false;
            }
            if (!Arrays.deepEquals(this.info, other.info)) {
                return false;
            }
            if (!Objects.equals(this.group, other.group)) {
                return false;
            }
            if (!Objects.equals(this.threshold, other.threshold)) {
                return false;
            }
            if (!Objects.equals(this.profiles, other.profiles)) {
                return false;
            }
            if (!Objects.equals(this.filterTags, other.filterTags)) {
                return false;
            }
            return true;
        }

    }

    public enum ProfileType {

        METRIC,
        AGGREGATION,
        OPERATIONS

    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 67 * hash + Objects.hashCode(this.tenantReport);
        hash = 67 * hash + Objects.hashCode(this.url);
        hash = 67 * hash + Objects.hashCode(this.jsonObject);
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
        final ReportParser other = (ReportParser) obj;
        if (!Objects.equals(this.url, other.url)) {
            return false;
        }
        if (!Objects.equals(this.tenantReport, other.tenantReport)) {
            return false;
        }
        if (!Objects.equals(this.jsonObject, other.jsonObject)) {
            return false;
        }
        return true;
    }

    

}
