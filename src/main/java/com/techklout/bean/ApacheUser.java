package com.techklout.bean;

import java.sql.Timestamp;

public class ApacheUser  implements java.io.Serializable {
    String asfid;
    String project;

    
    
	public String getAsfid() {
		return asfid;
	}
	public void setAsfid(String apacheId) {
		this.asfid = apacheId;
	}
	public String getProject() {
		return project;
	}
	public void setProject(String project) {
		this.project = project;
	}

    
    
}
