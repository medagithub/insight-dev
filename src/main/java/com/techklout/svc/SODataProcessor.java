package com.techklout.svc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.techklout.bean.ApacheUser;
import com.techklout.bean.Post;
import com.techklout.bean.PostSummaryInfo;
import com.techklout.bean.SOUser;
import com.techklout.bean.UserAcceptedAnswer;
import com.techklout.bean.UserPostTag;
import com.techklout.bean.UserPostTagVote;
import com.techklout.util.Config;

import com.techklout.util.TKUtil;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import redis.clients.jedis.Jedis;

public class SODataProcessor {
	static Logger logger = Logger.getLogger(SODataProcessor.class);

	static transient SparkConf conf = null;
	static JavaSparkContext context = null;
	static SQLContext sqlContext = null;
	static CassandraConnector connector = null;
	static Session session = null;


	static private boolean deleteDirectory(File path) {
		if (path.exists()) {
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++) {
				if (files[i].isDirectory()) {
					deleteDirectory(files[i]);
				} else {
					files[i].delete();
				}
			}
		}
		return (path.delete());
	}


	private static boolean initializeContexts() {
		long startTime = System.currentTimeMillis();
		try {
			conf = new SparkConf().setAppName(Config.getProperty("seed-data-processor-name")).setMaster(Config.getProperty("master-host", "local"));		     
			conf.set("spark.cassandra.connection.host", Config.getProperty("cassandra-host"));
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception while creating SparkConf!!!");
			return false;
		}

		try {
			context = new JavaSparkContext(conf);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception while creating JavaSparkContext!!!");
			return false;
		}


		session = null;
		try {
			// create cassandra connection/session
			connector = CassandraConnector.apply(context.getConf());
			session = connector.openSession();	

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception while creating Cassandra connector!!");
			logger.error(e.getMessage(),e);
			return false;
		}

		try {
			// create spark sql context
			sqlContext = new org.apache.spark.sql.SQLContext(context);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception while creating Spark SQL Context!!!");
			return false;
		}


		logger.debug("Successfully initialized all contexts!!!  Time taken = " + (System.currentTimeMillis() - startTime));
		return true;
	}


	// load Apache source foundation committers data
	static private boolean loadASFUserData() {
		long startTime = System.currentTimeMillis();
		String asfCommittersFile = Config.getProperty("asf-committer-seed-data-file");
		String usersFile = Config.getProperty("so-users-seed-data-file");
		if (usersFile == null || usersFile.isEmpty()) {
			logger.error("ASF User/Projects data file not found!!!");
			return false;
		}

		JavaRDD<String> asflines = null;
		JavaRDD<ApacheUser> asfline = null;
		DataFrame asf_df = null;
		try {
			// filter the lines that are not required
			asflines = context.textFile(asfCommittersFile);

			asfline = asflines.flatMap(PROCESS_ASF_COMMITTER_DATA);

			asf_df = sqlContext.createDataFrame(asfline, ApacheUser.class);
			asf_df.write().mode(SaveMode.Append)
			.option("keyspace", "tk_insight")
			.option("table", "asf_committer_projects")
			.format("org.apache.spark.sql.cassandra").save();
		} catch (Exception e) {
			logger.error("Exception while loading ASF User/Projects data!! Time taken - " + (System.currentTimeMillis() - startTime));
			logger.error(e.getMessage(), e);
			return false;
		}finally {
			if (asflines != null) asflines.unpersist(); 
			if (asfline != null) asfline.unpersist(); 
			if (asf_df != null) asf_df.unpersist(); 
		}
		logger.debug("Time taken to process apache users= " + (System.currentTimeMillis() - startTime));
		return true;
	}




	private static final FlatMapFunction<String, ApacheUser> PROCESS_ASF_COMMITTER_DATA =
			new FlatMapFunction<String, ApacheUser>() {
		public Iterable<ApacheUser> call(String s) throws Exception {
			List<ApacheUser> info = new ArrayList<ApacheUser>();
			try {
				if (s == null || s.isEmpty()) {
					logger.error("Invalid data!!! - " + s);
					return info;
				}
				StringTokenizer tokens = new StringTokenizer(s, "\t");
				if (tokens == null || tokens.countTokens() < 3){
					logger.debug("No projcts info found!!! - " + s);
					return info;
				}
				String userId = tokens.nextToken();
				String name = tokens.nextToken();
				String projects = tokens.nextToken();
				if (userId == null || userId.isEmpty() || projects == null || projects.isEmpty()) {
					logger.debug("No projects info found!!! - " + s);
					return info;
				}
				String[] strs = projects.split(",");
				for (int i = 0; i < strs.length;i++) {
					if (strs[i] == null || strs[i].isEmpty()) continue;
					ApacheUser user = new ApacheUser();
					user.setAsfid(userId.trim());
					user.setProject(strs[i].trim());
					info.add(user);
				}
			} catch (Exception e) {
				logger.error("Exception while processing apache committer info!! - " + s);
				logger.error(e.getMessage(), e);
			}
			return info;
		}
	};


	//load StackOverflow Users data
	static private boolean loadSOUserData() {
		long startTime = System.currentTimeMillis();

		String usersDir = Config.getProperty("so-users-dir");
		boolean isFilesUnderHdfs = Boolean.valueOf(Config.getProperty("is-files-under-hdfs", "false"));
		if (usersDir == null || usersDir.isEmpty()) {
			logger.error("SO User data Directory not found in the configuration!!!");
			return false;
		}

		// get the files in a sorted order based on timestamp
		String[] files = null;
		if (isFilesUnderHdfs) {
			files = TKUtil.listHdfsFiles(usersDir);
		} else {
			files = TKUtil.listFiles(usersDir);
		}

		if (files == null || files.length == 0) {
			logger.error("No files found under directory:" + usersDir);
			return false;
		}

		JavaRDD<String> userlines = null;
		JavaRDD<SOUser> userline = null;
		DataFrame sousers_df = null;

		//load one file at a time
		for (int i = 0; i < files.length; i++) {
			long start = System.currentTimeMillis();
			try {				
				userlines = context.textFile(usersDir + File.separator + files[i]);					  
				userline = userlines.map(SOUSER_PARSE_XML);		
				sousers_df = sqlContext.createDataFrame(userline, SOUser.class);
				sousers_df.write().mode(SaveMode.Append)
				.option("keyspace", "tk_insight")
				.option("table", "so_users")
				.format("org.apache.spark.sql.cassandra").save();
			} catch (Exception e) {
				logger.error("Exception while loading SO User data!! Time taken - " + (System.currentTimeMillis() - startTime));
				logger.error(e.getMessage(), e);
				return false;
			} finally {
				if (userlines != null) userlines.unpersist();
				if (userline != null) userline.unpersist();
				if (sousers_df != null) sousers_df.unpersist();
				logger.debug("Successfully laoded SO users from file:" + (usersDir + File.separator + files[i])  + "  Time taken = "+ (System.currentTimeMillis() - start));
			}
		}

		logger.debug("Successfully laoded SO users = " + (System.currentTimeMillis() - startTime));
		return true;

	}


	// for each post/answer, extract topic information and create UserPostTagVote instance for each topic found in the post.
	// UserPostTagVote instance stores the up/down votes for each topic for user
	private static final FlatMapFunction<String, UserPostTagVote> USER_TAG_VOTE_PARSE_XML =
			new FlatMapFunction<String, UserPostTagVote>() {
		public Iterable<UserPostTagVote> call(String s) throws Exception {

			UserPostTagVote[] userPostTagVotes = null;
			List<UserPostTagVote> info = new ArrayList<UserPostTagVote>();
			Jedis jedis  = null;
			try {
				jedis = new Jedis(Config.getProperty("redis-host", "localhost"), Integer.parseInt(Config.getProperty("redis-port", "6379")));
				InputStream is = new ByteArrayInputStream(s.getBytes());
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = null;
				try {
					doc = dBuilder.parse(is);
				} catch (Exception e) {
					logger.error("Error parsing - " + s);
					return info;
				}

				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("row");


				for (int temp = 0; temp < nList.getLength(); temp++) {

					Node nNode = nList.item(temp);					
					if (!"row".equals(nNode.getNodeName())) {
						logger.debug("Not interested in this string - " + s);
						return info;
					}
					NamedNodeMap attrs = nNode.getAttributes();
					if (attrs == null || attrs.getLength() == 0) {
						logger.error("Empty row!!! - " + s);
						return info;
					}              

					String id = "-1";
					String postId = "-1";
					String voteTypeId = "-1";
					String creationDate = null;


					if (attrs.getNamedItem("Id") != null)
						id = attrs.getNamedItem("Id").getNodeValue();


					if (attrs.getNamedItem("CreationDate") != null)
						creationDate = attrs.getNamedItem("CreationDate").getNodeValue();

					voteTypeId = getAttrValue(attrs, "VoteTypeId", voteTypeId);
					if (voteTypeId == null || voteTypeId.isEmpty()) {
						logger.error("VoteType info not found!! - "+ s);
						return info;
					}
					postId = getAttrValue(attrs, "PostId", postId);
					if (postId == null || postId.isEmpty()) {
						logger.error("PostId info not found!! - "+ s);
						return info;
					}


					//2014-04-17T00:49:28.617
					// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
					Timestamp date = null;
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					try {
						int index = creationDate.indexOf("T");
						if (index != -1) {
							creationDate = creationDate.substring(0, index);
						}
						date = new Timestamp(sdf.parse(creationDate).getTime());
					} catch (Exception e) {
						logger.error("Exception while parsing creation date - " + creationDate);
						return info;
					}


					String tags = "";
					String userId = "";
					// retrieve the topic/userid information from redis for this postId
					String psInfo = jedis.get(postId);
					if (psInfo == null || psInfo.isEmpty()) {
						logger.error("Post info not found in redis!! - " + s);
						return info;
					}
					int postTypeId = -1;
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
							tags = psInfo.substring(index + 1);
							if (tags == null || tags.isEmpty()) tags = "";                   	
							String[] strs = tags.split("><");
							List<UserPostTagVote> list = new ArrayList<UserPostTagVote>();
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

									UserPostTagVote userPostTagVote = new UserPostTagVote();											
									String user = psInfo.substring(0, index);
									if (user == null || user.isEmpty() || "-1".equals(user)) {
										logger.error("User info not found in redis !!! - " + psInfo);
										return info;
									}
									userPostTagVote.setUserId(Long.parseLong(user));
									userPostTagVote.setTag(strs[i]);
									userPostTagVote.setVoteTypeId(Integer.parseInt(voteTypeId));
									userPostTagVote.setDate(date);
									userPostTagVote.setPostTypeId(postTypeId);
									list.add(userPostTagVote);

								}  

								if (list.size() > 0) {
									return list;
								}

							}
						}

					}


					return info;

				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				if (jedis != null) {
					jedis.disconnect();
				}
			}
			return info;

		}
	};






	// parse the post event and create a Post instance for each post
	private static final Function<String, Post> POST_PARSE_XML =
			new Function<String, Post>() {
		public Post call(String s) throws Exception {

			Post post = new Post();
			Jedis jedis = null;
			try {
				jedis = new Jedis(Config.getProperty("redis-host", "localhost"), Integer.parseInt(Config.getProperty("redis-port", "6379")));
				InputStream is = new ByteArrayInputStream(s.getBytes());
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = null;
				try {
					doc = dBuilder.parse(is);
				} catch (Exception e) {
					logger.error("Error parsing - " + s);
					return post;
				}

				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("row");
				for (int temp = 0; temp < nList.getLength(); temp++) {

					Node nNode = nList.item(temp);					
					if (!"row".equals(nNode.getNodeName())) {
						logger.debug("Not interested in this string - " + s);
						return post;
					}
					NamedNodeMap attrs = nNode.getAttributes();
					if (attrs == null || attrs.getLength() == 0) {
						logger.debug("Empty row!!! - " + s);
						return post;
					}              

					String id = "-1";
					String postTypeId = "-1";
					String userId = "-1";
					String creationDate = null;
					String parentPostId = "-1";
					String acceptedAnswerId = "-1";
					String tags = "";
					String score = "0";
					String answerCount = "0";
					String viewCount = "0";
					String commentCount = "0";
					String favoriteCount = "0";


					if (attrs.getNamedItem("Id") != null)
						id = attrs.getNamedItem("Id").getNodeValue();

					if (id == null || id.isEmpty()) {
						logger.debug("PostId info not found - " + s);
						return post;
					}

					if (attrs.getNamedItem("PostTypeId") != null)
						postTypeId = attrs.getNamedItem("PostTypeId").getNodeValue();

					if (postTypeId == null || postTypeId.isEmpty()) {
						logger.debug(" PostTypeId info not found for post - " + s);
						return post;
					}

					if (!"1".equals(postTypeId) && !"2".equals(postTypeId)) {
						logger.debug("Not interested in this postTypeId - " + s);
						return post;
					}

					if (attrs.getNamedItem("OwnerUserId") != null)
						userId = attrs.getNamedItem("OwnerUserId").getNodeValue();

					if (userId == null || userId.isEmpty()) {
						logger.debug("Owner info not found - " + s);
						return post;
					}

					if (attrs.getNamedItem("CreationDate") != null)
						creationDate = attrs.getNamedItem("CreationDate").getNodeValue();

					if (creationDate == null || creationDate.isEmpty()) {
						logger.debug("Creation date not found for the post - " + s);
						return post;
					}

					if (attrs.getNamedItem("ParentId") != null)
						parentPostId = attrs.getNamedItem("ParentId").getNodeValue();

					if (attrs.getNamedItem("AcceptedAnswerId") != null)
						acceptedAnswerId = attrs.getNamedItem("AcceptedAnswerId").getNodeValue();

					if (attrs.getNamedItem("Tags") != null) {

						tags = attrs.getNamedItem("Tags").getNodeValue();
						if (tags == null || tags.isEmpty()) tags = "";                   	
						String[] strs = tags.split("><");
						ArrayList<String> list = new ArrayList<String>();
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
								list.add(strs[i]);

							}                   		

						}
						post.setTags(tags);
						post.setTagList(list);

					}
					if (attrs.getNamedItem("Score") != null)
						score = attrs.getNamedItem("Score").getNodeValue();

					if (attrs.getNamedItem("AnswerCount") != null) 
						answerCount = attrs.getNamedItem("AnswerCount").getNodeValue();

					if (attrs.getNamedItem("ViewCount") != null) 
						viewCount = attrs.getNamedItem("ViewCount").getNodeValue();

					if (attrs.getNamedItem("CommentCount") != null) 
						commentCount = attrs.getNamedItem("CommentCount").getNodeValue();

					if (attrs.getNamedItem("FavoriteCount") != null) 
						favoriteCount = attrs.getNamedItem("FavoriteCount").getNodeValue();

					post.setPostId(Long.parseLong(id));
					post.setPostTypeId(Integer.parseInt(postTypeId));
					post.setUserId(Long.parseLong(userId));
					post.setParentPostId(Long.parseLong(parentPostId));
					//2014-04-17T00:49:28.617
					// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					try {
						int index = creationDate.indexOf("T");
						if (index != -1) {
							creationDate = creationDate.substring(0, index);
						}
						post.setCreationDate(new Timestamp(sdf.parse(creationDate).getTime()));
					} catch (Exception e) {
						logger.error("Exception while parsing creation date - " + creationDate);
						return post;
					}

					post.setAcceptedAnswerId(Long.parseLong(acceptedAnswerId));
					post.setScore(Long.parseLong(score));
					post.setAnswerCount(Long.parseLong(answerCount));
					post.setViewCount(Long.parseLong(viewCount));
					post.setCommentCount(Long.parseLong(commentCount));
					post.setFavoriteCount(Long.parseLong(favoriteCount));
					if (post.getPostTypeId() == 1) { // question
						String value = jedis.set("" + post.getPostId(), "Q_" + userId + "_" + post.getTags());

					} else { //answer.  fetch the tag info from the appropriate question post in redis
						if ((post.getParentPostId() > 0) &&  (post.getTags() == null || post.getTags().isEmpty())) {
							String psInfo = (String)jedis.get("" + post.getParentPostId());						
							if (psInfo != null) {
								if (psInfo.startsWith("Q_") || psInfo.startsWith("A_")){
									psInfo = psInfo.substring(2);
								}
								int index = psInfo.indexOf("_");
								if (index != -1) {
									tags = psInfo.substring(index + 1);
									if (tags == null || tags.isEmpty()) tags = "";                   	
									String[] strs = tags.split("><");
									ArrayList<String> list = new ArrayList<String>();
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
											list.add(strs[i]);

										} 
										post.setTags(tags);
										post.setTagList(list);
										logger.debug("Set the tag info for answer post: " + post.getPostId() + " tags = " + post.getTags());
										String value = jedis.set("" + post.getPostId(), "A_" + userId + "_" + post.getTags());

									}
								}
							} 
						}


					}

					return post;

				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			} finally {
				if (jedis != null) {
					jedis.disconnect();
				}
			}
			return post;

		}
	};


	// for each post/answer, extract topic information and create UserPostTag instance for each topic found in the post.
	// UserPostTag instance stores the answer/question votes for each topic for user
	private static final FlatMapFunction<String, UserPostTag> FLAT_USER_POST_TAG3 =
			new FlatMapFunction<String, UserPostTag>() {
		public Iterable<UserPostTag> call(String s) throws Exception {


			Timestamp date = new Timestamp(System.currentTimeMillis());
			List<UserPostTag> info = new ArrayList<UserPostTag>();


			Jedis jedis = null;
			try {
				jedis = new Jedis(Config.getProperty("redis-host", "localhost"), Integer.parseInt(Config.getProperty("redis-port", "6379")));
				InputStream is = new ByteArrayInputStream(s.getBytes());
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = null;
				try {
					doc = dBuilder.parse(is);
				} catch (Exception e) {
					logger.error("Error parsing - " + s);
					return info;
				}

				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("row");
				for (int temp = 0; temp < nList.getLength(); temp++) {

					Node nNode = nList.item(temp);

					if (!"row".equals(nNode.getNodeName())) {
						logger.debug("Not interested in this string - " + s);
						return info;
					}
					NamedNodeMap attrs = nNode.getAttributes();
					if (attrs == null || attrs.getLength() == 0) {
						logger.error("Empty row!!! - " + s);
						return info;
					}      


					String id = "-1";
					String acceptedAnswerId = "-1";
					String postTypeId = "-1";
					String userId = "-1";
					String creationDate = null;
					String parentPostId = "-1";
					String tags = "";

					if (attrs.getNamedItem("Id") != null)
						id = attrs.getNamedItem("Id").getNodeValue();

					if (attrs.getNamedItem("PostTypeId") != null)
						postTypeId = attrs.getNamedItem("PostTypeId").getNodeValue();


					if (!"1".equals(postTypeId) && !"2".equals(postTypeId)) {
						logger.debug("Not interested in this string - " + s);
						return info;
					}

					if (attrs.getNamedItem("OwnerUserId") != null)
						userId = attrs.getNamedItem("OwnerUserId").getNodeValue();

					if (userId == null || userId.isEmpty()) {
						logger.debug("UserId info not found!! - " + s);
						return info;
					}

					if (attrs.getNamedItem("CreationDate") != null)
						creationDate = attrs.getNamedItem("CreationDate").getNodeValue();

					if (attrs.getNamedItem("ParentId") != null)
						parentPostId = attrs.getNamedItem("ParentId").getNodeValue();

					if (attrs.getNamedItem("AcceptedAnswerId") != null)
						acceptedAnswerId = attrs.getNamedItem("AcceptedAnswerId").getNodeValue();

					if (attrs.getNamedItem("Tags") != null) {
						tags = attrs.getNamedItem("Tags").getNodeValue();
					} 
					if ((tags == null || tags.isEmpty()) && parentPostId != null && "2".equals(postTypeId)) {
						// fetch the tag info from redis

						String psInfo = jedis.get(parentPostId);

						if (psInfo != null) {
							PostSummaryInfo psObj = new PostSummaryInfo(psInfo);
							tags = psObj.getTags();
						} else {
							logger.error("Post Info not found in redis!! - " + s);
							return info;
						}

					}

					if (tags != null && !tags.isEmpty()) {

						String[] strs = tags.split("><");
						if (strs != null && strs.length > 0) {

							for (int i = 0; i < strs.length; i++) {
								if (strs[i] == null || strs[i].isEmpty()) continue;
								strs[i] = strs[i].trim();
								if (strs[i].startsWith("<")) {
									strs[i] = strs[i].substring(1);
								}
								if (strs[i].endsWith(">")){
									strs[i] = strs[i].substring(0, strs[i].length()-1);
								}
								UserPostTag userPostTag = new UserPostTag();
								userPostTag.setUserid(Long.parseLong(userId));
								userPostTag.setTag(strs[i]);
								try {
									userPostTag.setPosttypeid(Integer.parseInt(postTypeId));
								} catch (Exception e) { }

								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
								try {
									int index = creationDate.indexOf("T");
									if (index != -1) {
										creationDate = creationDate.substring(0, index);
									}
									userPostTag.setDate(new Timestamp(sdf.parse(creationDate).getTime()));
								} catch (Exception e) {
									logger.error("Exception while parsing creation date - " + creationDate);
									continue;
								}

								if (userPostTag.getUserid() > 0 && (userPostTag.getTag() != null && !userPostTag.getTag().isEmpty()) && userPostTag.getDate() != null && userPostTag.getPosttypeid() > 0) {									
									info.add(userPostTag);
								} 
							}

							return info;

						}

					}  
				}
				return info;
			} catch (Exception e) {

				logger.error(e.getMessage(), e);
				return info;
			} finally {
				if (jedis != null) {
					jedis.disconnect();
				}

			}


		}
	};

	// process the question post with AccepterAnswerId
	private static final FlatMapFunction<String, UserAcceptedAnswer> PROCESS_ACCEPTED_ANSWERS =
			new FlatMapFunction<String, UserAcceptedAnswer>() {
		public Iterable<UserAcceptedAnswer> call(String s) throws Exception {

			Timestamp date = new Timestamp(System.currentTimeMillis());
			List<UserAcceptedAnswer> info = new ArrayList<UserAcceptedAnswer>();


			Jedis jedis = null;
			try {
				jedis = new Jedis(Config.getProperty("redis-host", "localhost"), Integer.parseInt(Config.getProperty("redis-port", "6379")));
				InputStream is = new ByteArrayInputStream(s.getBytes());
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = null;
				try {
					doc = dBuilder.parse(is);
				} catch (Exception e) {
					logger.error("Error parsing - " + s);
					return info;
				}
				//	  System.out.println("2");
				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("row");
				for (int temp = 0; temp < nList.getLength(); temp++) {
					//	System.out.println("3");
					Node nNode = nList.item(temp);
					//System.out.println("nodeType = " + nNode.getNodeType() + "  node name = " + nNode.getNodeName() );
					if (!"row".equals(nNode.getNodeName())) {
						logger.debug("Not interested in this string - " + s);
						return info;
					}
					NamedNodeMap attrs = nNode.getAttributes();
					if (attrs == null || attrs.getLength() == 0) {
						logger.error("Empty row!!! - " + s);
						return info;
					}      
					//	System.out.println("4");

					String id = "-1";
					String postTypeId = "-1";
					String userId = "-1";
					String creationDate = null;
					String parentPostId = "-1";
					String acceptedAnswerId = "-1";
					String tags = "";

					if (attrs.getNamedItem("Id") != null)
						id = attrs.getNamedItem("Id").getNodeValue();

					if (attrs.getNamedItem("PostTypeId") != null)
						postTypeId = attrs.getNamedItem("PostTypeId").getNodeValue();

					if (!"1".equals(postTypeId) ) {
						return info;
					}

					if (attrs.getNamedItem("OwnerUserId") != null)
						userId = attrs.getNamedItem("OwnerUserId").getNodeValue();



					if (attrs.getNamedItem("CreationDate") != null)
						creationDate = attrs.getNamedItem("CreationDate").getNodeValue();

					if (attrs.getNamedItem("ParentId") != null)
						parentPostId = attrs.getNamedItem("ParentId").getNodeValue();

					if (attrs.getNamedItem("AcceptedAnswerId") != null)
						acceptedAnswerId = attrs.getNamedItem("AcceptedAnswerId").getNodeValue();

					if ("1".equals(postTypeId) && (acceptedAnswerId == null || acceptedAnswerId.isEmpty())) {
						return info;
					}

					if (attrs.getNamedItem("Tags") != null) {
						tags = attrs.getNamedItem("Tags").getNodeValue();
					} 
					if ((tags == null || tags.isEmpty()) ) {
						// fetch the tag info from redis

						String psInfo = jedis.get(id);

						if (psInfo != null) {
							PostSummaryInfo psObj = new PostSummaryInfo(psInfo);
							tags = psObj.getTags();
						} else {
							logger.error("Tag info not found for post!! - " + s);
							return info;
						}

					}

					long acceptedAnswerUserId = -1;
					if (tags != null && !tags.isEmpty()) {
						String pInfo = jedis.get(acceptedAnswerId);
						if (pInfo != null) {
							PostSummaryInfo pObj = new PostSummaryInfo(pInfo);
							if (pObj != null && pObj.getUserId() > 0) {
								acceptedAnswerUserId = pObj.getUserId();
							} 
						} 
						if (acceptedAnswerUserId <= 0){
							logger.error("User Info not found for AcceptedAnswerId!! - " + s + "  pInfo = " + pInfo);
							return info;
						}

						String[] strs = tags.split("><");
						if (strs != null && strs.length > 0) {

							for (int i = 0; i < strs.length; i++) {
								if (strs[i] == null || strs[i].isEmpty()) continue;
								strs[i] = strs[i].trim();
								if (strs[i].startsWith("<")) {
									strs[i] = strs[i].substring(1);
								}
								if (strs[i].endsWith(">")){
									strs[i] = strs[i].substring(0, strs[i].length()-1);
								}
								UserAcceptedAnswer userPostTag = new UserAcceptedAnswer();
								userPostTag.setUserId(acceptedAnswerUserId);
								userPostTag.setTag(strs[i]);


								SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
								try {
									int index = creationDate.indexOf("T");
									if (index != -1) {
										creationDate = creationDate.substring(0, index);
									}
									userPostTag.setDate(new Timestamp(sdf.parse(creationDate).getTime()));
								} catch (Exception e) {
									logger.error("Exception while parsing creation date - " + creationDate);
									continue;
								}

								if (userPostTag.getUserId() > 0 && (userPostTag.getTag() != null && !userPostTag.getTag().isEmpty()) && userPostTag.getDate() != null ) {								
									info.add(userPostTag);
								} 	
							}

							return info;

						}

					}  
				}
				return info;
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				return info;
			} finally {
				if (jedis != null) {
					jedis.disconnect();
				}

			}


		}
	};


	// utility methos to check attribite value
	static String getAttrValue(NamedNodeMap map, String attrName, String defaultValue) {
		if (map != null && attrName != null  && map.getNamedItem(attrName) != null) {
			return map.getNamedItem(attrName).getNodeValue();
		}
		return defaultValue;
	}

	// process SO user data
	private static final Function<String, SOUser> SOUSER_PARSE_XML =
			new Function<String, SOUser>() {
		public SOUser call(String s) throws Exception {

			SOUser user = new SOUser();

			try {

				InputStream is = new ByteArrayInputStream(s.getBytes());
				DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = null;
				try {
					doc = dBuilder.parse(is);
				} catch (Exception e) {
					logger.error("Error parsing - " + s);
					return user;
				}

				doc.getDocumentElement().normalize();

				NodeList nList = doc.getElementsByTagName("row");
				for (int temp = 0; temp < nList.getLength(); temp++) {

					Node nNode = nList.item(temp);

					if (!"row".equals(nNode.getNodeName())) {
						logger.debug("Not interested in this string - " + s);
						return user;
					}
					NamedNodeMap attrs = nNode.getAttributes();
					if (attrs == null || attrs.getLength() == 0) {
						logger.error("Empty row!!! - " + s);
						return user;
					}              

					String userId = "-1";
					String creationDate = null;
					String reputation = "0";
					String name = null;
					String lastAccessDate = null;
					String location = null;
					String totalUpVotes = "0";
					String totalDownVotes = "0";
					String totalViews = "0";
					String email = null;
					String seId = "-1";
					String age = "-1";


					if (attrs.getNamedItem("Id") != null)
						userId = attrs.getNamedItem("Id").getNodeValue();


					if (attrs.getNamedItem("CreationDate") != null)
						creationDate = attrs.getNamedItem("CreationDate").getNodeValue();

					reputation = getAttrValue(attrs, "Reputation", reputation);
					name = getAttrValue(attrs, "DisplayName", name);
					lastAccessDate = getAttrValue(attrs, "LastAccessDate", lastAccessDate);

					location = getAttrValue(attrs, "location", location);
					totalUpVotes = getAttrValue(attrs, "UpVotes", totalUpVotes);
					totalDownVotes = getAttrValue(attrs, "DownVotes", totalDownVotes);
					totalViews = getAttrValue(attrs, "Views", totalViews);
					age = getAttrValue(attrs, "Age",age);
					seId = getAttrValue(attrs, "AccountId",seId);
					email = getAttrValue(attrs, "EmailHash", email);

					// System.out.println(id + "," + name + "," + gender);

					user = new SOUser();

					user.setUserid(Long.parseLong(userId));
					user.setReputation(Long.parseLong(reputation));
					user.setName(name);
					user.setLocation(location);

					user.setAge(Integer.parseInt(age));

					user.setEmail(email);
					return user;

				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			return user;

		}
	};



	//main method
	public static void main(String[] args) {

		long start = System.currentTimeMillis();
		long startTime = System.currentTimeMillis();
		String outputDir = null;	
		String postsDir = null;
		String usersDir = null;
		String votesDir = null;
		String asfCommittersFile = null;

		JavaRDD<String> qlines = null;
		JavaRDD<String> alines = null;
		JavaRDD<Post> qposts = null;
		JavaRDD<Post> aposts = null;
		DataFrame user_view_counts_df = null;
		DataFrame qposts_df = null;
		DataFrame aposts_df = null;

		JavaRDD<UserPostTag> rowRDD2 = null;
		JavaRDD<UserPostTag> rowRDD3 = null;
		DataFrame userposttag_df = null;
		DataFrame userposttag_df2 = null;
		DataFrame userposttagcounts_df = null;
		DataFrame userposttagcounts_df2 = null;

		JavaRDD<UserAcceptedAnswer>  acceptedAnswers = null;
		DataFrame acceptedAnswers_df = null;
		DataFrame acceptedAnswers_df2 = null;
		DataFrame tag_user_questions_counts_df = null;
		DataFrame user_post_tag_votes_counts_df = null;
		DataFrame tag_user_answers_counts_df  =  null;

		try {

			// read the seed data file name from configuration
			outputDir = Config.getProperty("output-dir", "");	
			postsDir = Config.getProperty("so-posts-dir");
			usersDir = Config.getProperty("so-users-dir");
			votesDir = Config.getProperty("so-votes-dir");

			asfCommittersFile = Config.getProperty("asf-committer-seed-data-file");

			try {
				conf = new SparkConf().setAppName(Config.getProperty("seed-data-processor-name")).setMaster(Config.getProperty("master-host", "local"));		     
				conf.set("spark.cassandra.connection.host", Config.getProperty("cassandra-host"));
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Exception while creating SparkConf!!!");
				return;
			}

			try {
				context = new JavaSparkContext(conf);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Exception while creating JavaSparkContext!!!");
				return;
			}


			session = null;
			try {
				// create cassandra connection/session
				connector = CassandraConnector.apply(context.getConf());
				session = connector.openSession();	

			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Exception while creating Cassandra connector!!");
				logger.error(e.getMessage(),e);
				return;
			}

			try {
				// create spark sql context
				sqlContext = new org.apache.spark.sql.SQLContext(context);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error("Exception while creating Spark SQL Context!!!");
				return;
			}


			logger.debug("Successfully initialized all contexts!!!  Time taken = " + (System.currentTimeMillis() - startTime));

			// delete any output directory so process doesn't fail when you are running in local mode
			deleteDirectory(new File(outputDir));

			loadSOUserData();
			loadASFUserData();

			startTime = System.currentTimeMillis();

			postsDir = Config.getProperty("so-posts-dir");
			boolean isFilesUnderHdfs = Boolean.valueOf(Config.getProperty("is-files-under-hdfs", "false"));
			if (postsDir == null || postsDir.isEmpty()) {
				logger.error("SO Posts Data Directory not found in the configuration!!!");
				return;
			}

			String[] files = null;
			if (isFilesUnderHdfs) {
				files = TKUtil.listHdfsFiles(postsDir);
			} else {
				files = TKUtil.listFiles(postsDir);
			}

			if (files == null || files.length == 0) {
				logger.error("No files found under directory:" + postsDir);
				return;
			}


			// process post file one at a time
			for (int i = 0; i < files.length; i++) {
				start = System.currentTimeMillis();
				String fn = postsDir + File.separator + files[i];
				try {

					// filter the lines that are not required
					qlines = context.textFile(fn).filter(
							new Function<String, Boolean>() {
								public Boolean call(String s) {
									return (!s.contains("<posts>") && !s.contains("</posts>") && !s.contains("<?xml") && s.contains(" OwnerUserId=\"") && (s.contains("PostTypeId=\"1\"") ));
								}
							});

					startTime = System.currentTimeMillis();
					qposts = qlines.map(POST_PARSE_XML);
					qposts_df = sqlContext.createDataFrame(qposts, Post.class);
					qposts_df.registerTempTable("qposts");
					logger.debug("No. of questions posts to process:" + qposts.count()); 
					qposts.cache();
					qlines.cache();


					// filter the lines that are not required
					alines = context.textFile(fn).filter(
							new Function<String, Boolean>() {
								public Boolean call(String s) {
									return (!s.contains("<posts>") && !s.contains("</posts>") && !s.contains("<?xml") && s.contains(" OwnerUserId=\"") && (s.contains("PostTypeId=\"2\"") ));
								}
							});
					alines.cache();

					startTime = System.currentTimeMillis();
					aposts = alines.map(POST_PARSE_XML);
					logger.debug("No. of answer posts to process:" + aposts.count()); 

					startTime = System.currentTimeMillis();

					user_view_counts_df = sqlContext.sql("select creationDate as date, userId as userid, sum(viewCount) as cnt from qposts group by creationDate, userId");
					user_view_counts_df.registerTempTable("user_post_view_count");
					user_view_counts_df.printSchema();
					user_view_counts_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "user_post_view_count")
					.format("org.apache.spark.sql.cassandra").save();
					logger.debug("Time taken to process user_post_view_count = " + (System.currentTimeMillis() - startTime));



					startTime = System.currentTimeMillis();
					// Convert records of the RDD  to Rows.
					rowRDD2 = qlines.flatMap(FLAT_USER_POST_TAG3);	
					userposttag_df = sqlContext.createDataFrame(rowRDD2, UserPostTag.class);
					userposttag_df.printSchema();
					userposttag_df.registerTempTable("userposttags");
					userposttag_df.printSchema();

					userposttagcounts_df = sqlContext.sql("select date, userid, tag, posttypeid, count(*) as cnt "
							+ " from userposttags group by date, userid, tag, posttypeid ");

					userposttagcounts_df.registerTempTable("user_post_tags");

					userposttagcounts_df.printSchema();

					userposttagcounts_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "user_post_tags")
					.format("org.apache.spark.sql.cassandra").save();



					startTime = System.currentTimeMillis();
					// Convert records of the RDD  to Rows.
					rowRDD3 = alines.flatMap(FLAT_USER_POST_TAG3);	
					userposttag_df2 = sqlContext.createDataFrame(rowRDD3, UserPostTag.class);
					userposttag_df2.printSchema();

					userposttag_df2.registerTempTable("userposttags2");
					userposttag_df2.printSchema();

					userposttagcounts_df2 = sqlContext.sql("select date, userid, tag, posttypeid, count(*) as cnt "
							+ " from userposttags2 group by date, userid, tag, posttypeid ");

					userposttagcounts_df2.registerTempTable("user_post_tags2");


					userposttagcounts_df2.printSchema();

					userposttagcounts_df2.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "user_post_tags")
					.format("org.apache.spark.sql.cassandra").save();


					logger.debug("Time taken to process user post tags = " + (System.currentTimeMillis() - startTime));

					startTime = System.currentTimeMillis();
					// Convert records of the RDD  to Rows.
					acceptedAnswers = qlines.flatMap(PROCESS_ACCEPTED_ANSWERS);	
					acceptedAnswers_df = sqlContext.createDataFrame(acceptedAnswers, UserAcceptedAnswer.class);
					acceptedAnswers_df.printSchema();

					acceptedAnswers_df.registerTempTable("useracceptedanswers");

					acceptedAnswers_df2 = sqlContext.sql("select date, userId as userid, tag, count(*) as cnt "
							+ " from useracceptedanswers group by date, userId, tag");

					acceptedAnswers_df2.registerTempTable("user_accepted_answers");

					acceptedAnswers_df2.printSchema();

					acceptedAnswers_df2.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "user_accepted_answers")
					.format("org.apache.spark.sql.cassandra").save();



					logger.debug("Time taken to process accepted anserwers = " + (System.currentTimeMillis() - startTime) + "  count = " + acceptedAnswers_df2.count());
					startTime = System.currentTimeMillis();

					tag_user_questions_counts_df = sqlContext.sql("select tag, userid, sum(cnt) as cnt "
							+ " from user_post_tags where posttypeid=1 group by tag, userid ");

					tag_user_questions_counts_df.registerTempTable("tag_user_questions");
					tag_user_questions_counts_df.printSchema();

					tag_user_questions_counts_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "tag_user_questions")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get tag_user_questions = " + (System.currentTimeMillis() - startTime));

					startTime = System.currentTimeMillis();

					tag_user_answers_counts_df = sqlContext.sql("select tag, userid, sum(cnt) as cnt "
							+ " from user_post_tags2 where posttypeid=2 group by tag, userid ");

					tag_user_answers_counts_df.registerTempTable("tag_user_answers");
					tag_user_answers_counts_df.printSchema();

					tag_user_answers_counts_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "tag_user_answers")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get tag_user_answers = " + (System.currentTimeMillis() - startTime));

					TKUtil.moveHdfsFile(fn, postsDir + "_done");
				} catch (Exception e) {
					logger.error("Exception while loading SO Posts data!! Time taken - " + (System.currentTimeMillis() - startTime));
					logger.error(e.getMessage(), e);

				} finally {
					if (qlines != null) qlines.unpersist();
					if (alines != null) alines.unpersist();
					if (qposts != null) qposts.unpersist();
					if (aposts != null) aposts.unpersist();
					if (user_view_counts_df != null) user_view_counts_df.unpersist();
					if (qposts_df != null) qposts_df.unpersist();
					if (aposts_df != null) aposts_df.unpersist();;

					if (rowRDD2 != null) rowRDD2.unpersist();;
					if (rowRDD3 != null) rowRDD3.unpersist();;
					if (userposttag_df != null) userposttag_df.unpersist();;
					if (userposttag_df2 != null) userposttag_df2.unpersist();;
					if (userposttagcounts_df != null) userposttagcounts_df.unpersist();;
					if (userposttagcounts_df2 != null) userposttagcounts_df2.unpersist();;

					if (acceptedAnswers != null) acceptedAnswers.unpersist();
					if (acceptedAnswers_df != null) acceptedAnswers_df.unpersist();;
					if (acceptedAnswers_df2 != null) acceptedAnswers_df2.unpersist();;
					if (tag_user_questions_counts_df != null) tag_user_questions_counts_df.unpersist();;
					if (user_post_tag_votes_counts_df != null) user_post_tag_votes_counts_df.unpersist();;
					if (tag_user_answers_counts_df  !=  null) tag_user_answers_counts_df.unpersist();;
					if (userposttagcounts_df != null) userposttagcounts_df.unpersist();
					if (acceptedAnswers_df2 != null) acceptedAnswers_df2.unpersist();
					logger.debug("Successfully laoded SO Posts from file:" + (postsDir + File.separator + files[i])  + "  Time taken = "+ (System.currentTimeMillis() - start) + "  Free Memory  " + Runtime.getRuntime().freeMemory() + "  Total Memory = " + Runtime.getRuntime().totalMemory());
					startTime = System.currentTimeMillis();
					try {
						System.gc();
					} catch (Exception e) { }
					logger.debug("Time taken to run GC - " + (System.currentTimeMillis() - startTime)  + "  Total free memory - " + Runtime.getRuntime().freeMemory() + "  Total Memory = " + Runtime.getRuntime().totalMemory());

				}
			}

			logger.debug("Successfully laoded SO Posts = " + (System.currentTimeMillis() - startTime));



			startTime = System.currentTimeMillis();

			votesDir = Config.getProperty("so-votes-dir");
			isFilesUnderHdfs = Boolean.valueOf(Config.getProperty("is-files-under-hdfs", "false"));
			if (votesDir == null || votesDir.isEmpty()) {
				logger.error("SO Votes data Directory not found in the configuration!!!");
				return;
			}

			files = null;
			if (isFilesUnderHdfs) {
				files = TKUtil.listHdfsFiles(votesDir);
			} else {
				files = TKUtil.listFiles(votesDir);
			}

			if (files == null || files.length == 0) {
				logger.error("No files found under directory:" + votesDir);
				return;
			}

			JavaRDD<String> votelines = null;
			JavaRDD<UserPostTagVote> voteline = null;
			DataFrame user_post_tag_vote_df = null;
			DataFrame tag_user_up_qvotes_df = null;
			DataFrame tag_user_down_qvotes_df = null;
			DataFrame tag_user_down_avotes_df = null;
			for (int i = 0; i < files.length; i++) {
				start = System.currentTimeMillis();
				String fn = votesDir + File.separator + files[i];
				try {

					votelines = context.textFile(fn);	
					votelines.cache();
					voteline = votelines.flatMap(USER_TAG_VOTE_PARSE_XML);	
					voteline.cache();
					user_post_tag_vote_df = sqlContext.createDataFrame(voteline, UserPostTagVote.class);
					user_post_tag_vote_df.registerTempTable("userposttagvotes");
					user_post_tag_vote_df.printSchema();

					logger.debug("Time taken to process user_post_tag_vote data = " + (System.currentTimeMillis() - startTime));

					startTime = System.currentTimeMillis();

					user_post_tag_votes_counts_df = sqlContext.sql("select date, userId as userid, tag, postTypeId as posttypeid, voteTypeId as votetypeid, count(*) as cnt "
							+ " from userposttagvotes group by date, userId, tag, postTypeId, voteTypeId ");


					user_post_tag_votes_counts_df.registerTempTable("user_post_tag_votes");

					user_post_tag_votes_counts_df.printSchema();
					user_post_tag_votes_counts_df.cache();

					user_post_tag_votes_counts_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "user_post_tag_votes")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get user_post_tag_votes = " + (System.currentTimeMillis() - startTime) );

					startTime = System.currentTimeMillis();

					user_post_tag_votes_counts_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "tag_post_user_votes")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get tag_post_user_votes = " + (System.currentTimeMillis() - startTime) );

					startTime = System.currentTimeMillis();

					tag_user_up_qvotes_df = sqlContext.sql("select tag, userid, sum(cnt) as cnt "
							+ " from user_post_tag_votes where posttypeid=1 and votetypeid=2 group by tag, userid ");

					tag_user_up_qvotes_df.registerTempTable("tag_user_up_qvotes");
					tag_user_up_qvotes_df.printSchema();
					tag_user_up_qvotes_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "tag_user_up_qvotes")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get tag_user_up_qvotes = " + (System.currentTimeMillis() - startTime));

					startTime = System.currentTimeMillis();

					tag_user_down_qvotes_df = sqlContext.sql("select tag, userid, sum(cnt) as cnt "
							+ " from user_post_tag_votes where posttypeid=1 and votetypeid=3 group by tag, userid ");

					tag_user_down_qvotes_df.registerTempTable("tag_user_down_qvotes");
					tag_user_down_qvotes_df.printSchema();
					//	user_post_tag_votes.show(10);
					tag_user_down_qvotes_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "tag_user_down_qvotes")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get tag_user_down_qvotes = " + (System.currentTimeMillis() - startTime));

					startTime = System.currentTimeMillis();
					DataFrame tag_user_up_avotes_df = sqlContext.sql("select tag, userid, sum(cnt) as cnt "
							+ " from user_post_tag_votes where posttypeid=2 and votetypeid=2 group by tag, userid");

					tag_user_up_avotes_df.registerTempTable("tag_user_up_avotes");
					tag_user_up_avotes_df.printSchema();
					//	user_post_tag_votes.show(10);
					tag_user_up_avotes_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "tag_user_up_avotes")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get tag_user_up_avotes = " + (System.currentTimeMillis() - startTime));

					startTime = System.currentTimeMillis();
					tag_user_down_avotes_df = sqlContext.sql("select tag, userid, sum(cnt) as cnt "
							+ " from user_post_tag_votes where posttypeid=2 and votetypeid=3 group by tag, userid");

					tag_user_down_avotes_df.registerTempTable("tag_user_down_avotes");
					tag_user_down_avotes_df.printSchema();
					tag_user_down_avotes_df.write().mode(SaveMode.Append)
					.option("keyspace", "tk_insight")
					.option("table", "tag_user_down_avotes")
					.format("org.apache.spark.sql.cassandra").save();

					logger.debug("Time taken to get tag_user_down_avotes = " + (System.currentTimeMillis() - startTime));

					TKUtil.moveHdfsFile(fn, votesDir + "_done");	  

				} catch (Exception e) {
					logger.error("Exception while loading SO Votes data!! Time taken - " + (System.currentTimeMillis() - startTime));
					logger.error(e.getMessage(), e);

				} finally {
					if (votelines != null) votelines.unpersist();
					if (voteline != null) voteline.unpersist();
					if (user_post_tag_vote_df != null) user_post_tag_vote_df.unpersist();
					if (tag_user_up_qvotes_df != null) tag_user_up_qvotes_df.unpersist();
					if (tag_user_down_qvotes_df != null) tag_user_down_qvotes_df.unpersist();
					if (tag_user_down_avotes_df != null) tag_user_down_avotes_df.unpersist();
					if (user_post_tag_votes_counts_df != null) user_post_tag_votes_counts_df.unpersist();
					logger.debug("Successfully laoded SO Votes from file:" + (votesDir + File.separator + files[i])  + "  Time taken = "+ (System.currentTimeMillis() - start) + "  Free Memory  " + Runtime.getRuntime().freeMemory() + "  Total Memory = " + Runtime.getRuntime().totalMemory());


					startTime = System.currentTimeMillis();
					try {
						System.gc();
					} catch (Exception e) { }
					logger.debug("Time taken to run GC - " + (System.currentTimeMillis() - startTime)  + "  Total free memory - " + Runtime.getRuntime().freeMemory() + "  Total Memory = " + Runtime.getRuntime().totalMemory());

				}
			}

			logger.debug("Successfully laoded SO Votes = " + (System.currentTimeMillis() - startTime));

		} catch (Exception e) {
			logger.error("Exception while processing seed data!!!");
			logger.error(e.getMessage(), e);
		} finally {
			if (session != null) session.close();


			if (context != null) context.stop();


			logger.debug("Time taken  = " + (System.currentTimeMillis() - start));

		}
	}

}
