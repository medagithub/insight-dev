package com.techklout.bean;

import java.sql.Timestamp;

public class UserPostTagVote  implements java.io.Serializable {
	long userId;
	String tag;
	Timestamp date;
	int postTypeId;
	int voteTypeId;
	

	
	public int getVoteTypeId() {
		return voteTypeId;
	}
	public void setVoteTypeId(int voteTypeId) {
		this.voteTypeId = voteTypeId;
	}
	public int getPostTypeId() {
		return postTypeId;
	}
	public void setPostTypeId(int postTypeId) {
		this.postTypeId = postTypeId;
	}
	public long getUserId() {
		return userId;
	}
	public void setUserId(long userId) {
		this.userId = userId;
	}
	public String getTag() {
		return tag;
	}
	public void setTag(String tagId) {
		this.tag = tagId;
	}
	public Timestamp getDate() {
		return date;
	}
	public void setDate(Timestamp date) {
		this.date = date;
	}
	

}
