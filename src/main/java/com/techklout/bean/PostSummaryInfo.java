package com.techklout.bean;

import java.util.ArrayList;
import java.util.List;

public class  PostSummaryInfo {
	Long postId;
	List<String> tagList;
	Long userId;
	int postTypeId;
	String tags;
	
	public PostSummaryInfo(String psInfo) {
		if (psInfo != null) {
			if (psInfo.startsWith("Q_")){
				psInfo = psInfo.substring(2);
				postTypeId = 1;
			}
			if (psInfo.startsWith("A_")){
				psInfo = psInfo.substring(2);
				postTypeId = 2;
			}
			
			int index = psInfo.indexOf("_");
			if (index != -1) {	
				userId = Long.parseLong(psInfo.substring(0, index));
				tags = psInfo.substring(index + 1);
				if (tags == null || tags.isEmpty()) tags = "";                   	
				String[] strs = tags.split("><");
				tagList = new ArrayList<String>();
				if (strs != null && strs.length > 0) {
					for (int i = 0; i < strs.length; i++) {
						
						if (strs[i] == null) continue;
						strs[i] = strs[i].trim();
						if (strs[i].startsWith("<") ) {
							strs[i] = strs[i].substring(1);
						}
						if (strs[i].endsWith(">")) {
							strs[i] = strs[i].substring(0, strs[i].length()-1);
						}
											
						tagList.add(strs[i]);

					}  
					
				}
			}
			
		}
		
		if (userId < 0) {
			System.out.println("************************************ invalid user id !!! psInfo = " + psInfo);
		}
	}
	
	
	public int getPostTypeId() {
		return postTypeId;
	}
	public void setPostTypeId(int postTypeId) {
		this.postTypeId = postTypeId;
	}
	public String getTags() {
		return tags;
	}
	public void setTags(String tags) {
		this.tags = tags;
	}
	public Long getPostId() {
		return postId;
	}
	public void setPostId(Long postId) {
		this.postId = postId;
	}
	public List<String> getTagList() {
		return tagList;
	}
	public void setTagList(List<String> tags) {
		this.tagList = tags;
	}
	public Long getUserId() {
		return userId;
	}
	public void setUserId(Long userId) {
		this.userId = userId;
	}
	


}
