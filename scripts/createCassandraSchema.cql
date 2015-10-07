DROP KEYSPACE IF EXISTS tk_insight;
CREATE KEYSPACE if not exists tk_insight WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
use tk_insight;
				
//Total view count per user per day
CREATE TABLE if not exists tk_insight.user_post_view_count (date TIMESTAMP, userid bigint, cnt bigint, PRIMARY KEY (userid, date)) with clustering order by (date desc);

//Total questions/answers per topic per user per day (searchable based on user)
CREATE TABLE if not exists tk_insight.user_post_tags (date TIMESTAMP, userid bigint, tag text, posttypeid int,  cnt bigint, PRIMARY KEY (userid, tag, posttypeid, date)) with clustering order by (tag asc, posttypeid asc, date desc);
	
//Total Votes per questions/answers per topic per user per day (searchable based on userid)
CREATE TABLE if not exists tk_insight.user_post_tag_votes (date TIMESTAMP, userid bigint, tag text, posttypeid int, votetypeid int, cnt bigint, PRIMARY KEY (userid, tag, posttypeid, votetypeid, date)) with clustering order by (tag asc, posttypeid asc, votetypeid asc, date desc);
CREATE TABLE if not exists tk_insight.tag_post_user_votes (date TIMESTAMP, userid bigint, tag text, posttypeid int, votetypeid int, cnt bigint, PRIMARY KEY (tag, posttypeid, votetypeid, date)) with clustering order by (posttypeid asc, votetypeid asc, date desc);

//Total questions per topic per user  (searchable based on topic)
create table if not exists tk_insight.tag_user_questions(tag text, userid bigint, cnt bigint, primary key(tag, userid)) with clustering order by (userid asc);

//Total answers per topic per user (searchable based on topic)
create table if not exists tk_insight.tag_user_answers(tag text, userid bigint, cnt bigint, primary key(tag, cnt, userid)) with clustering order by (cnt desc, userid asc);

//Total accepted answers per topic per user (searchable based on user)
create table if not exists tk_insight.user_accepted_answers(date TIMESTAMP, userid bigint, tag text, cnt bigint, primary key(userid, tag, date)) with clustering order by (tag asc, date desc);

//Total UP votes for questions per topic per user(searchable based on topic)
create table if not exists tk_insight.tag_user_up_qvotes(tag text, userid bigint, cnt bigint, primary key(tag, cnt, userid)) with clustering order by (cnt desc, userid asc);

//Total DOWN votes for questions per topic per user(searchable based on topic)
create table if not exists tk_insight.tag_user_down_qvotes(tag text, userid bigint, cnt bigint, primary key(tag, cnt, userid)) with clustering order by (cnt desc, userid asc);

//Total UP votes for answers per topic per user(searchable based on topic)
create table if not exists tk_insight.tag_user_up_avotes(tag text, userid bigint, cnt bigint, primary key(tag, cnt, userid)) with clustering order by (cnt desc, userid asc);

//Total DOWN votes for answers per topic per user(searchable based on topic)
create table if not exists tk_insight.tag_user_down_avotes(tag text, userid bigint, cnt bigint, primary key(tag, cnt, userid)) with clustering order by (cnt desc, userid asc);		
				
CREATE TABLE if not exists tk_insight.so_users (userid bigint, name text, age int, reputation bigint, location text, email text, PRIMARY KEY(userid, reputation)) with clustering order by (reputation desc);
CREATE TABLE if not exists tk_insight.asf_committer_projects (asfid text, project text,  PRIMARY KEY(asfid, project)) with clustering order by (project asc);
CREATE TABLE if not exists tk_insight.tk_users (userid text, password text,  name text, creationdate timestamp, souserid text, asfuserid text, hruserid text, PRIMARY KEY(userid, creationdate)) with clustering order by (creationdate asc);

