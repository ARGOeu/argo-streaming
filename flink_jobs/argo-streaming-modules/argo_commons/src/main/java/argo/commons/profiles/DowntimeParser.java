/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.commons.profiles;

import argo.commons.requests.RequestManager;
import argo.commons.timelines.Utils;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 *
 * @author cthermolia
 */
public class DowntimeParser implements Serializable {

    ArrayList<Endpoint> downtimeEndpoints = new ArrayList<>();
    private HashMap<String, String[]> downtimes = new HashMap<>();
    private final String url = "/downtimes";
    private String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
     private JSONObject jsonObject;
    public DowntimeParser() {
    }

    public DowntimeParser(String apiUri, String key, String proxy, String date) throws IOException, ParseException, java.text.ParseException {
        String uri = apiUri + url;
        if (date != null) {
            uri = uri + "?date=" + date;
        }
        loadDowntimesProfile(uri, key, proxy);
    }

    public DowntimeParser(JSONObject jsonObject) throws java.text.ParseException {
        this.jsonObject = jsonObject;
        readApiRequestResult();
    }

    private void loadDowntimesProfile(String uri, String key, String proxy) throws IOException, org.json.simple.parser.ParseException, java.text.ParseException {
         jsonObject = RequestManager.request(uri, key, proxy);
         readApiRequestResult();
    }
      public void readApiRequestResult() throws java.text.ParseException{
        JSONArray data = (JSONArray) jsonObject.get("data");

        JSONObject dataObject = (JSONObject) data.get(0);

        //   JSONArray jsonArray=dataObject.get("services");
        Iterator<Object> dataIter = data.iterator();
        while (dataIter.hasNext()) {
            Object dataobj = dataIter.next();
            if (dataobj instanceof JSONObject) {
                JSONObject jsonDataObj = new JSONObject((Map) dataobj);

                JSONArray endpointsArray = (JSONArray) jsonDataObj.get("endpoints");

                Iterator<Object> iterator = endpointsArray.iterator();

                while (iterator.hasNext()) {
                    Object obj = iterator.next();
                    if (obj instanceof JSONObject) {
                        JSONObject endpObj = new JSONObject((Map) obj);
                        String hostname = (String) endpObj.get("hostname");
                        String service = (String) endpObj.get("service");
                        String startT = (String) endpObj.get("start_time");
                        String endT = (String) endpObj.get("end_time");

                        Date startTime = Utils.convertStringtoDate(format, startT);
                        Date endTime = Utils.convertStringtoDate(format, endT);
                        downtimes.put(hostname+"-"+service, new String[]{startT,endT});
                        Endpoint endpoint = new Endpoint(hostname, service, startTime, endTime);
                        downtimeEndpoints.add(endpoint);
                    }
                }
            }
        }
    }

    public JSONObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public HashMap<String, String[]> getDowntimes() {
        return downtimes;
    }

    public void setDowntimes(HashMap<String, String[]> downtimes) {
        this.downtimes = downtimes;
    }
      

    public class Endpoint implements Serializable {

        private String hostname;
        private String service;
        private Date startTime;
        private Date endTime;

        public Endpoint() {
        }

        public Endpoint(String hostname, String service, Date startTime, Date endTime) {
            this.hostname = hostname;
            this.service = service;
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public Date getStartTime() {
            return startTime;
        }

        public void setStartTime(Date startTime) {
            this.startTime = startTime;
        }

        public Date getEndTime() {
            return endTime;
        }

        public void setEndTime(Date endTime) {
            this.endTime = endTime;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 53 * hash + Objects.hashCode(this.hostname);
            hash = 53 * hash + Objects.hashCode(this.service);
            hash = 53 * hash + Objects.hashCode(this.startTime);
            hash = 53 * hash + Objects.hashCode(this.endTime);
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
            final Endpoint other = (Endpoint) obj;

            if (!Objects.equals(this.hostname, other.hostname)) {
                System.out.println("false hostname");
                return false;
            }
            if (!Objects.equals(this.service, other.service)) {
                System.out.println("false service");
                return false;
            }
            if (!Objects.equals(this.startTime, other.startTime)) {
                System.out.println("false starttime");
                return false;
            }
            if (!Objects.equals(this.endTime, other.endTime)) {
                System.out.println("false endtime");
                return false;
            }
            return true;
        }

    }

    public ArrayList<Endpoint> getDowntimeEndpoints() {
        return downtimeEndpoints;
    }

    public void setDowntimeEndpoints(ArrayList<Endpoint> downtimeEndpoints) {
        this.downtimeEndpoints = downtimeEndpoints;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String[] retrieveDownTimes(String hostname, String service) {
      return downtimes.get(hostname+"-"+service);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 41 * hash + Objects.hashCode(this.downtimeEndpoints);
        hash = 41 * hash + Objects.hashCode(this.downtimes);
        hash = 41 * hash + Objects.hashCode(this.url);
        hash = 41 * hash + Objects.hashCode(this.format);
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
        final DowntimeParser other = (DowntimeParser) obj;
        if (!Objects.equals(this.url, other.url)) {
            System.out.println("this url ---- "+this.url+" other url ---- "+other.url);
            return false;
        }
        if (!Objects.equals(this.format, other.format)) {
            System.out.println("this format ---- "+this.format+" other format ---- "+other.format);
            return false;
        }
        if (!Objects.equals(this.downtimeEndpoints, other.downtimeEndpoints)) {
            System.out.println("this downtimeEndpoints ---- "+this.downtimeEndpoints+" other downtimeEndpoints ---- "+other.downtimeEndpoints);
            return false;
        }
        if (!Objects.equals(this.downtimes, other.downtimes)) {
            System.out.println("this downtimes ---- "+this.downtimes+" other downtimes ---- "+other.downtimes);
            return false;
        }
        return true;
    }

}
