package com.techklout.util;
import java.io.*;
import java.util.StringTokenizer;


public class PopulateTKUserData {
	
	public static void main(String[] args)  throws Exception{
		BufferedReader rd = null;
				
		try {
			rd = new BufferedReader(new FileReader(new File("/Users/Jyothi/apache-committers.tsv")));
			String line = null;
			int count = 1;
			while ((line = rd.readLine()) != null) {
				int index = line.indexOf(" ");
				
				//System.out.println("index = " + index + "  line = " +  line);
				if (index == -1) continue;
				String username = line.substring(0, index);
				System.out.println("INSERT INTO tk_insight.tk_users (userid, password, name, creationdate, souserid, asfuserid, hruserid) VALUES ('user" + count + "', 'password" + count + "', 'User " + count + "', '2015-09-25 00:00:00', '" + count + "', '" + username + "', '');");
				count++;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rd != null) rd.close();
		}
	}

}
