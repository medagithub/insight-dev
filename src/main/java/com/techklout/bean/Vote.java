package com.techklout.bean;

import java.io.Serializable;
import java.sql.Timestamp;

public class Vote implements  java.io.Serializable {
	long voteId;
	long postId;
	int voteTypeId;
	Timestamp creationDate;
	
	public long getVoteId() {
		return voteId;
	}
	public void setVoteId(long id) {
		this.voteId = id;
	}
	public long getPostId() {
		return postId;
	}
	public void setPostId(long postId) {
		this.postId = postId;
	}
	public int getVoteTypeId() {
		return voteTypeId;
	}
	public void setVoteTypeId(int voteTypeId) {
		this.voteTypeId = voteTypeId;
	}
	public Timestamp getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}
	
	

}
