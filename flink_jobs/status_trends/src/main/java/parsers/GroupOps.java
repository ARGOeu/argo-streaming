/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package parsers;

import java.util.HashMap;

/**
 *
 * @author cthermolia
 */
public class GroupOps {

    private String name;
    private String operation;
    private HashMap<String, String> services;

    public GroupOps(String name, String operation, HashMap<String, String> services) {
        this.name = name;
        this.operation = operation;
        this.services = services;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public HashMap<String, String> getServices() {
        return services;
    }

    public void setServices(HashMap<String, String> services) {
        this.services = services;
    }
    
    

    
}
