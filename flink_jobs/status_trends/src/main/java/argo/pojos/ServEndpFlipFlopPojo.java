/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package argo.pojos;

/**
 *
 * @author cthermolia
 */
public class ServEndpFlipFlopPojo {

    String group;
    String service;

    String hostname;
    Integer flipflops;

    public ServEndpFlipFlopPojo() {
    }

    
    public ServEndpFlipFlopPojo(String group, String service, String hostname, Integer flipflops) {
        this.group = group;
        this.service = service;
        this.hostname = hostname;
        this.flipflops = flipflops;
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

    public Integer getFlipflops() {
        return flipflops;
    }

    public void setFlipflops(Integer flipflops) {
        this.flipflops = flipflops;
    }

}
