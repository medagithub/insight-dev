package com.techklout.bean;


import java.sql.Timestamp;

public class UserPostTag  implements java.io.Serializable  {
	  

		long userid;
		String tag ;
		Timestamp date;
		int posttypeid;

		

		public int getPosttypeid() {
			return posttypeid;
		}
		public void setPosttypeid(int postTypeId) {
			this.posttypeid = postTypeId;
		}
		public long getUserid() {
			return userid;
		}
		public void setUserid(long userId) {
			this.userid = userId;
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
