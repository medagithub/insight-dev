package com.techklout.bean;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

public class Post implements Serializable {
	Timestamp creationDate;
	Long postId;
	Integer postTypeId;
	Long userId;
	Long parentPostId;
	Long acceptedAnswerId;
	String tags;
	Long score;
	Long answerCount;
	Long commentCount;
	Long favoriteCount;
	Long viewCount;

    List<String> tagList;

	public List<String> getTagList() {
		return tagList;
	}
	public void setTagList(List<String> tagList) {
		this.tagList = tagList;
	}
	public Timestamp getCreationDate() {
		return creationDate;
	}
	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}
	public Long getPostId() {
		return postId;
	}
	public void setPostId(Long postId) {
		this.postId = postId;
	}
	public Integer getPostTypeId() {
		return postTypeId;
	}
	public void setPostTypeId(Integer postTypeId) {
		this.postTypeId = postTypeId;
	}
	public Long getUserId() {
		return userId;
	}
	public void setUserId(Long userId) {
		this.userId = userId;
	}
	public Long getParentPostId() {
		return parentPostId;
	}
	public void setParentPostId(Long parentPostId) {
		this.parentPostId = parentPostId;
	}
	public Long getAcceptedAnswerId() {
		return acceptedAnswerId;
	}
	public void setAcceptedAnswerId(Long acceptedAnswerId) {
		this.acceptedAnswerId = acceptedAnswerId;
	}
	public String getTags() {
		return tags;
	}
	public void setTags(String tags) {
		this.tags = tags;
	}
	public Long getScore() {
		return score;
	}
	public void setScore(Long score) {
		this.score = score;
	}
	public Long getAnswerCount() {
		return answerCount;
	}
	public void setAnswerCount(Long answerCount) {
		this.answerCount = answerCount;
	}
	public Long getCommentCount() {
		return commentCount;
	}
	public void setCommentCount(Long commentCount) {
		this.commentCount = commentCount;
	}
	public Long getFavoriteCount() {
		return favoriteCount;
	}
	public void setFavoriteCount(Long favoriteCount) {
		this.favoriteCount = favoriteCount;
	}
	public Long getViewCount() {
		return viewCount;
	}
	public void setViewCount(Long viewCount) {
		this.viewCount = viewCount;
	}
	
	
	
}