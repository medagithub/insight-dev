package com.techklout.util;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import redis.clients.jedis.*;

public class TKRedisClient {
	static Logger logger = Logger.getLogger(TKRedisClient.class);
	static JedisCluster jc = null;
	static Jedis jedis = null;
	static Set<HostAndPort> jedisClusterNode = null;
	//static TKRedisClient client = new TKRedisClient();
	static String redisHost = "localhost";
	static int redisPort = 6379;
			
	
//	static {
//		  try {
//			 
//			  GenericObjectPoolConfig config = new GenericObjectPoolConfig();
//			  int maxTotal = 0;
//			  try {
//				  maxTotal = Integer.parseInt(Config.getProperty("redis-cluster-max-total", "0"));
//			  } catch (Exception e) {
//				  logger.error("Invalid value for 'redis-cluster-max-total'.  Using default value 2000");
//			  }
//			  config.setMaxTotal(maxTotal);
//			  
//			  
//			  int maxWaitInMillis = 2000;
//			  try {
//				  maxWaitInMillis = Integer.parseInt(Config.getProperty("redis-cluster-max-wait-in-millis", "2000"));
//			  } catch (Exception e) {
//				  logger.error("Invalid value for 'redis-cluster-max-wait-in-millis'.  Using default value 2000");
//			  }
//			  config.setMaxWaitMillis(maxWaitInMillis);
//			  
//			  String redisClusterHostAndPorts = Config.getProperty("redis-cluster-host-and-ports", "localhost:6379");
//			  String[] hostsAndPorts = null;
//			  jedisClusterNode = new HashSet<HostAndPort>();
//			  if (redisClusterHostAndPorts != null && !redisClusterHostAndPorts.isEmpty()) {
//				  hostsAndPorts = redisClusterHostAndPorts.split(",");
//				  for (int i=0; i < hostsAndPorts.length; i++) {
//					  int index = hostsAndPorts[i].indexOf(":");
//					  if (index == -1) continue;
//					  String host = hostsAndPorts[i].substring(0, index);
//					  int port = 6379;
//					  try {
//						  port = Integer.parseInt(hostsAndPorts[i].substring(index+1));
//					  } catch (Exception e) {
//						  logger.error("Invalid port - " + hostsAndPorts[i] + "  Using default port 6379");
//					  }
//					 
//					  jedisClusterNode.add(new HostAndPort(host, port));
//					  logger.debug("Added Redis host:" + host + "  Port:" + port);
//				  }
//				  if (jedisClusterNode.size() == 0) {
//					  jedisClusterNode.add(new HostAndPort("localhost", 6379));
//					  logger.debug("****Added default Redis host - localhost:6379");
//				  }
//			  } else {
//				  jedisClusterNode.add(new HostAndPort("localhost", 6379));
//				  logger.debug("++++++Added default Redis host - localhost:6379");
//			  }
//			  
//			  jc = new JedisCluster(jedisClusterNode, config);
//		  } catch (Exception e) {
//			  logger.error("Exception while creating redis cluster!!!");
//			  logger.error(e.getMessage(), e);
//		  }
//	}
	
	private TKRedisClient() {
		redisHost = Config.getProperty("redis-host", "localhost");
		try {
			redisPort = Integer.parseInt(Config.getProperty("redis-port"));
		} catch (Exception e) {
			redisPort = 6379;
		}
		try {
			jedis = new Jedis(redisHost, redisPort);
			
		} catch(Exception e) {
			logger.error("Exception while creating jedis!!! host - " + redisHost + "  port:" + redisPort);
		}
	}
	
	public static synchronized Jedis getRedis() {
		return jedis;
	}

}
