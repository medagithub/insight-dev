package com.techklout.util;

import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Config {
    private static Logger logger = Logger.getLogger(Config.class);
	private static Properties props = new Properties();
	private static String cfgFileName = "techklout.properties";
	private static Config instance = new Config();
	//load config properties
	static {
		long start = System.currentTimeMillis();
		InputStream is = null;
		try {
			is = Config.class.getClassLoader().getResourceAsStream(cfgFileName);
			if (is == null) {
				logger.error("Could not load config properties from file:" + cfgFileName);				
			} else {
				props.load(is);
				logger.info("Successfully loaded config properties from file:" + cfgFileName + "  Time taken = " + (System.currentTimeMillis() - start));
				logger.info(props.toString());
			}
		} catch (Exception e) {
			logger.error("Exception while loading config properties from file:" + cfgFileName);
			logger.error(e.getMessage(), e);
			
		}
	}
	
	private Config() {
		
	}
	
	public static String getProperty(String name) {
		return (String)props.get(name);
	}
	
	public static String getProperty(String name, String defaultValue) {
		Object obj = props.get(name);
		if (obj == null) return defaultValue;
		return (String) obj;
	}

}
