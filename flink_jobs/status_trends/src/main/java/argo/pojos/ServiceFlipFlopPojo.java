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
public class ServiceFlipFlopPojo {

    String profileName;
   String group;
    String service;
    Integer flipflops;

    public ServiceFlipFlopPojo() {
    }

    
    public ServiceFlipFlopPojo(String profileName, String group, String service, Integer flipflops) {
        this.profileName = profileName;
        this.group=group;
        this.service = service;
        this.flipflops = flipflops;
    }


    public String getService() {
        return service;
    }

    public Integer getFlipflops() {
        return flipflops;
    }

    public String getProfileName() {
        return profileName;
    }

    public void setProfileName(String profileName) {
        this.profileName = profileName;
    }


    public void setService(String service) {
        this.service = service;
    }

    public void setFlipflops(Integer flipflops) {
        this.flipflops = flipflops;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
    

}
