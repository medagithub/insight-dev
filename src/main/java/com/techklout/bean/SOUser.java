package com.techklout.bean;

import java.io.Serializable;
import java.sql.Timestamp;

public class SOUser  implements Serializable {
   long userid;
//   Timestamp creationDate;
   long reputation;
   String name;
//   Timestamp lastAccessDate;
   String location;
//   long totalUpVotes;
 //  long totalDownVotes;
  // long totalViews;
   String email;
 //  long seId;
   int age;
  
 
   
   
//public long getTotalViews() {
//	return totalViews;
//}
//public void setTotalViews(long totalViews) {
//	this.totalViews = totalViews;
//}
public long getUserid() {
	return userid;
}
public void setUserid(long userId) {
	this.userid = userId;
}
//public Timestamp getCreationDate() {
//	return creationDate;
//}
//public void setCreationDate(Timestamp creationDate) {
//	this.creationDate = creationDate;
//}
public long getReputation() {
	return reputation;
}
public void setReputation(long reputation) {
	this.reputation = reputation;
}
public String getName() {
	return name;
}
public void setName(String name) {
	this.name = name;
}
//public Timestamp getLastAccessDate() {
//	return lastAccessDate;
//}
//public void setLastAccessDate(Timestamp lastAccessDate) {
//	this.lastAccessDate = lastAccessDate;
//}
public String getLocation() {
	return location;
}
public void setLocation(String location) {
	this.location = location;
}
//public long getTotalUpVotes() {
//	return totalUpVotes;
//}
//public void setTotalUpVotes(long totalUpVotes) {
//	this.totalUpVotes = totalUpVotes;
//}
//public long getTotalDownVotes() {
//	return totalDownVotes;
//}
//public void setTotalDownVotes(long totalDownVotes) {
//	this.totalDownVotes = totalDownVotes;
//}
public String getEmail() {
	return email;
}
public void setEmail(String email) {
	this.email = email;
}
//public long getSeId() {
//	return seId;
//}
//public void setSeId(long seId) {
//	this.seId = seId;
//}
public int getAge() {
	return age;
}
public void setAge(int age) {
	this.age = age;
}
   
   
   
   
}
