package com.techklout.bean;

import java.sql.Timestamp;

public class TKUser  implements java.io.Serializable {
	
	   String userid;
	   Timestamp creationdate;	  
	   long souserid;
	   String asfuserid;
	   String hruserid;	   
	   String name;
	   
//	   Timestamp lastAccessDate;
//	   long tkScore;
//	   String street1;
//	   String street2;
//	   String city;
//	   String state;
//	   String country;
//	   String zipcode;	   
//	   String email;
//	   int age;
	  
		   
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userId) {
		this.userid = userId;
	}
	public Timestamp getCreationdate() {
		return creationdate;
	}
	public void setCreationdate(Timestamp creationDate) {
		this.creationdate = creationDate;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
//	public long getReputation() {
//		return tkScore;
//	}
//	public void setReputation(long reputation) {
//		this.tkScore = reputation;
//	}
//
//	public Timestamp getLastAccessDate() {
//		return lastAccessDate;
//	}
//	public void setLastAccessDate(Timestamp lastAccessDate) {
//		this.lastAccessDate = lastAccessDate;
//	}
//
//	public String getEmail() {
//		return email;
//	}
//	public void setEmail(String email) {
//		this.email = email;
//	}
//
//	public int getAge() {
//		return age;
//	}
//	public void setAge(int age) {
//		this.age = age;
//	}
//	public long getTkScore() {
//		return tkScore;
//	}
//	public void setTkScore(long tkScore) {
//		this.tkScore = tkScore;
//	}
	public long getSouserid() {
		return souserid;
	}
	public void setSouserid(long soId) {
		this.souserid = soId;
	}
	public String getAsfuserid() {
		return asfuserid;
	}
	public void setAsfuserid(String apacheId) {
		this.asfuserid = apacheId;
	}
	public String getHruserid() {
		return hruserid;
	}
	public void setHruserid(String hrId) {
		this.hruserid = hrId;
	}
//	public String getStreet1() {
//		return street1;
//	}
//	public void setStreet1(String street1) {
//		this.street1 = street1;
//	}
//	public String getStreet2() {
//		return street2;
//	}
//	public void setStreet2(String street2) {
//		this.street2 = street2;
//	}
//	public String getCity() {
//		return city;
//	}
//	public void setCity(String city) {
//		this.city = city;
//	}
//	public String getState() {
//		return state;
//	}
//	public void setState(String state) {
//		this.state = state;
//	}
//	public String getCountry() {
//		return country;
//	}
//	public void setCountry(String country) {
//		this.country = country;
//	}
//	public String getZipcode() {
//		return zipcode;
//	}
//	public void setZipcode(String zipcode) {
//		this.zipcode = zipcode;
//	}
	  
}
