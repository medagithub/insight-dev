package com.techklout.bean;


import java.sql.Timestamp;

public class UserAcceptedAnswer  implements java.io.Serializable  {
	  

		long userId;
		String tag ;
		Timestamp date;


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
