from flask import jsonify 
from app import app
from cassandra.cluster import Cluster
from flask import render_template
from flask import request
import json
import time
#importing Cassandra modules from the driver we just installed

# setting up connections to cassandra

cluster = Cluster(['ec2-54-215-137-241.us-west-1.compute.amazonaws.com']) 
session = cluster.connect('tk_insight')

@app.route('/')

@app.route('/index')

def index():
  user = { 'nickname': 'XYZ' } # fake user
  return render_template("login.html", title = 'Home')

@app.route('/slides')

def slides():
  return render_template("slides.html", title = 'TechKlout')

@app.route('/login')

def login():

  return render_template("login.html", title = 'Login')


def validate_user(userid, password):
	stmt = "SELECT * FROM tk_users WHERE userid=%s";
        response = session.execute(stmt, parameters=[userid])
        response_list = []
        for val in response:
            response_list.append(val)

        for x in response_list:
            if (x.password == password):
                print "Successful login!!! userid:" + userid
                return {"so_userid": x.souserid, "asf_userid": x.asfuserid}
        print "Unsuccessful login!!! userid = " + userid
        return 


def calc_score(so_userid, asf_userid):
        stmt = "SELECT reputation FROM so_users WHERE userid=%s";
        response = session.execute(stmt, parameters=[long(so_userid)])
        response_list = []
        for val in response:
            response_list.append(val)

        so_score = 0
        for x in response_list:
            so_score = x.reputation
    
        stmt = "SELECT * from asf_committer_projects where asfid=%s"
        response = session.execute(stmt, parameters=[asf_userid])
        asf_score = 0
        for val in response:
            asf_score = asf_score + 100
 
        score = so_score + asf_score
        return score

@app.route("/login", methods=['POST'])

def fetch_user_info():
    usermetrics = get_user_metrics();
    return render_template("userinfo.html", output=usermetrics)

#get list of apache projects associated
def get_apache_projects(asfId):
    stmt = "SELECT * FROM asf_committer_projects WHERE asfid=%s"
    response = session.execute(stmt, parameters=[asfId])

    response_list = []
    for val in response:
        response_list.append(val.project)
    return response_list 
 
#get the accepted answer count for each topic
def get_accepted_answer_count_per_topic(so_userid):
    stmt = "SELECT * FROM user_accepted_answers WHERE userid=%s"
    response = session.execute(stmt, parameters=[long(so_userid)])

    response_list = []
    for val in response:
        response_list.append(val)

    accepted_answers = {}
    for x in response_list:
          if (accepted_answers.get(x.tag) == None):
              accepted_answers[x.tag] = 0
          accepted_answers[x.tag] = accepted_answers[x.tag] + x.cnt
    return accepted_answers; 

# get count for question/answer for each topic
def get_question_answer_count_per_topic(so_userid):
    stmt = "SELECT * FROM user_post_tags WHERE userid=%s"
    response = session.execute(stmt, parameters=[long(so_userid)])

    response_list = []
    for val in response:
        response_list.append(val)

    question_answer = {}
    answers = {}
    questions = {}
    for x in response_list:
          if (x.posttypeid == 1):
              if (questions.get(x.tag) == None):
                  questions[x.tag] = 0
              questions[x.tag] = questions[x.tag] + x.cnt
          else:
              if (answers.get(x.tag) == None):
                  answers[x.tag] = 0
              answers[x.tag] = answers[x.tag] + x.cnt
              
    question_answer['questions'] = questions; 
    question_answer['answers'] = answers; 
    return question_answer;

#calculate the percent for each topic
def get_topic_percent(topic_answers):
    total = 0
    topic_percent_list = []
    for key, value in topic_answers.items():
        total = total + value

    percent = 0.0
    for key, value in topic_answers.items():
        percentlist = []
        if (value > 0):
            percent = float(float(float(value)/float(total)) * 100)
            if (percent > 0.0):
                percentlist.append(key.encode('ascii', 'ignore'))
                percentlist.append(percent)
                topic_percent_list.append(percentlist)
    return topic_percent_list
    

#calculate percentile for each topic
def calc_topic_percentile(distinct_tag_info_list):
    count = 0;
    totalusers_withbelow_count = 0;
    totalusers_withequal_count = 0;
    totalusers = 0;
    topic_percentile_list = []
 
    
    
    for key, value in distinct_tag_info_list.items():
        tag = key.encode('ascii', 'ignore') 
        if (value.get('answers') > 0):
            stmt = "SELECT count(*) as count FROM tag_user_answers WHERE tag=%s and cnt<%s"
            response = session.execute(stmt, parameters=[tag, value.get('answers')])

 
            for val in response:
                totalusers_withbelow_count = val.count;
 
            
            stmt = "SELECT count(*) as count FROM tag_user_answers WHERE tag=%s"
            response = session.execute(stmt, parameters=[tag])

            for val in response:
                totalusers = val.count;

            percentile = 0.0
            if (totalusers_withbelow_count > 0):
                percentile = float(totalusers_withbelow_count)/float(totalusers) * 100.0
            templist = []
            templist.append(tag)
            templist.append(percentile)
            topic_percentile_list.append(templist)

    return topic_percentile_list


# Fetch all information regarding user
# fetch questions/answers/up votes/down votes
# calculate the topic percent  
def get_user_metrics():

    userid = request.form["userid"]

    password = request.form["password"]

    resp = validate_user(userid, password);
    if (not resp):
        print "validate user response is either null or empty!!!"
        return render_template("login.html", loginfailed="User authentication failed.")

    userinfo = resp
    print "**********userinfo*************"
    print userinfo['so_userid']
    print userinfo['asf_userid']
    score = calc_score(userinfo['so_userid'], userinfo['asf_userid'])
    print "**********************************Score: %d" % score

    asf_projects = get_apache_projects(userinfo['asf_userid']);
    topic_accepted_answers = get_accepted_answer_count_per_topic(userinfo['so_userid']);
    topic_question_answers = get_question_answer_count_per_topic(userinfo['so_userid']);
    starttime = time.time();
    stmt = "SELECT * FROM user_post_tag_votes WHERE userid=%s"
    response = session.execute(stmt, parameters=[long(userinfo['so_userid'])])
    elapsed = time.time() - starttime;
    print "***** time taken to get data from user_post_tag_votes for user:" + userid + " = " + str(elapsed)
    starttime = time.time();

    response_list = []
    for val in response:
        response_list.append(val)

    jsonresponse = {}
    total_questions = 0
    total_answers = 0
    total_accepted_answers = 0
    total_upvotes = 0
    total_downvotes = 0
    topic_questions = topic_question_answers.get('questions')
    topic_answers = topic_question_answers.get('answers')
    topic_question_upvotes = {}
    topic_question_downvotes = {}
    topic_answer_upvotes = {}
    topic_answer_downvotes = {}
    
    distinct_tag_info = {}
        
    for x in response_list:
           if (distinct_tag_info.get(x.tag) == None):
              distinct_tag_info[x.tag] = {}
              distinct_tag_info.get(x.tag)['questions'] = 0
              distinct_tag_info.get(x.tag)['answers'] = 0
              distinct_tag_info.get(x.tag)['accepted_answers'] = 0
              distinct_tag_info.get(x.tag)['question_upvotes'] = 0
              distinct_tag_info.get(x.tag)['question_downvotes'] = 0
              distinct_tag_info.get(x.tag)['answer_upvotes'] = 0
              distinct_tag_info.get(x.tag)['answer_downvotes'] = 0
     
  
           if (x.posttypeid == 1):
              distinct_tag_info.get(x.tag)['questions'] = distinct_tag_info.get(x.tag).get('questions') + x.cnt;
              
              
              if (x.votetypeid == 2):
                  total_upvotes = total_upvotes + x.cnt
                  if (topic_question_upvotes.get(x.tag) == None):
                      topic_question_upvotes[x.tag] = 0
                  topic_question_upvotes[x.tag] = topic_question_upvotes[x.tag] + x.cnt
                  distinct_tag_info.get(x.tag)['question_upvotes'] = distinct_tag_info.get(x.tag).get('question_upvotes') + x.cnt;
              else:
                  total_downvotes = total_downvotes + x.cnt
                  if (topic_question_downvotes.get(x.tag) == None):
                      topic_question_downvotes[x.tag] = 0
                  topic_question_downvotes[x.tag] = topic_question_downvotes[x.tag] + x.cnt
                  distinct_tag_info.get(x.tag)['question_downvotes'] = distinct_tag_info.get(x.tag).get('question_downvotes') + x.cnt;
           else:
              distinct_tag_info.get(x.tag)['answers'] = distinct_tag_info.get(x.tag).get('answers') + x.cnt
              if (distinct_tag_info.get(x.tag).get('accepted_answers') == None): 
                  distinct_tag_info.get(x.tag)['accepted_answers'] = accepted_answers.get(x.tag)


              if (x.votetypeid == 2):
                  total_upvotes = total_upvotes + x.cnt
                  if (topic_answer_upvotes.get(x.tag) == None):
                      topic_answer_upvotes[x.tag] = 0
                  topic_answer_upvotes[x.tag] = topic_answer_upvotes[x.tag] + x.cnt
                  distinct_tag_info.get(x.tag)['answer_upvotes'] = distinct_tag_info.get(x.tag).get('answer_upvotes') + x.cnt;
              else:
                  total_downvotes = total_downvotes + x.cnt
                  if (topic_answer_downvotes.get(x.tag) == None):
                      topic_answer_downvotes[x.tag] = 0
                  topic_answer_downvotes[x.tag] = topic_answer_downvotes[x.tag] + x.cnt
                  distinct_tag_info.get(x.tag)['answer_downvotes'] = distinct_tag_info.get(x.tag).get('answer_downvotes') + x.cnt;

     
    elapsed = time.time() - starttime;
    print "***** time taken to iterate thru user_post_tag_votes data for user:" + userid + " = " + str(elapsed)

    
    starttime = time.time()
    topic_percentile_list = calc_topic_percentile(distinct_tag_info)
    #topic_percentile_list = []
    elapsed = time.time() - starttime;
    print "***** time taken to calculate percentiles for topics for user:" + userid + " = " + str(elapsed)
    starttime = time.time()
    topic_questions_list = []
    for key, value in topic_questions.items():
        templist=[]
        templist.append(key.encode('ascii','ignore'))
        templist.append(value)
        topic_questions_list.append(templist) 
        total_questions = total_questions + value
    
    topic_answers_list = []
    for key, value in topic_answers.items():
        templist=[]
        templist.append(key.encode('ascii','ignore'))
        templist.append(value)
        topic_answers_list.append(templist) 
        total_answers = total_answers + value
       

    topic_accepted_answers_list = []
    total_accepted_answers = 0
    for key, value in topic_accepted_answers.items():
        templist=[]
        templist.append(key.encode('ascii','ignore'))
        templist.append(value)
        topic_accepted_answers_list.append(templist)
        total_accepted_answers = total_accepted_answers + value
    
    topic_question_upvotes_list = []
    for key, value in topic_question_upvotes.items():
        templist=[]
        templist.append(key.encode('ascii','ignore'))
        templist.append(value)
        topic_question_upvotes_list.append(templist) 
    
    topic_question_downvotes_list = []
    for key, value in topic_question_downvotes.items():
        templist=[]
        templist.append(key.encode('ascii','ignore'))
        templist.append(value)
        topic_question_downvotes_list.append(templist) 
    
    topic_answer_upvotes_list = []
    for key, value in topic_answer_upvotes.items():
        templist=[]
        templist.append(key.encode('ascii','ignore'))
        templist.append(value)
        topic_answer_upvotes_list.append(templist) 
    
    topic_answer_downvotes_list = []
    for key, value in topic_answer_downvotes.items():
        templist=[]
        templist.append(key.encode('ascii','ignore'))
        templist.append(value)
        topic_answer_downvotes_list.append(templist) 
    
    distinct_tag_info_list = []
    distinct_tag_question_list = []
    distinct_tag_answer_list = []
    distinct_tag_acceptedanswer_list = []
    distinct_tag_vote_list = []
    distinct_tag_upvote_list = []
    distinct_tag_downvote_list = []
    distinct_topic_list=[]
    tag_percent_list = get_topic_percent(topic_answers)
    
    for key, value in distinct_tag_info.items():
        templist=[]
        templist2=[]
        percentlist=[]
        
        total = value.get('questions') + value.get('answers') + value.get('accepted_answers')
        if (total > 0):
           distinct_topic_list.append(key.encode('ascii','ignore'))
           templist.append(key.encode('ascii','ignore'))
           templist.append(value.get('questions'))
           templist.append(value.get('answers'))
           templist.append(value.get('accepted_answers'))
           temp = 0

           if (topic_accepted_answers.get(key.encode('ascii', 'ignore')) == None):
               temp = 0;
           else:
               temp = topic_accepted_answers.get(key.encode('ascii', 'ignore'))
           templist.append(temp);
           distinct_tag_question_list.append(value.get('questions'))
           distinct_tag_answer_list.append(value.get('answers'))
           distinct_tag_acceptedanswer_list.append(temp)
           distinct_tag_info_list.append(templist) 
           

           percent = 0.0
           if ((value.get('answers') + temp) > 0):
               percent = float(float(float(value.get('answers') + temp )/float(total_answers + total_accepted_answers)) * 100)

           if (percent > 0) :
               percentlist.append(key.encode('ascii', 'ignore'))
               percentlist.append(percent)
               #tag_percent_list.append(percentlist)
           
           total = value.get('question_upvotes') + value.get('question_downvotes') + value.get('answer_upvotes') + value.get('answer_downvotes')
           if (total > 0):
               templist2 = []
               templist2.append(key.encode('ascii','ignore'))
               templist2.append(value.get('question_upvotes'))
               templist2.append(value.get('question_downvotes'))
               templist2.append(value.get('answer_upvotes'))
               templist2.append(value.get('answer_downvotes'))
               distinct_tag_vote_list.append(templist2) 


    elapsed = time.time() - starttime;
    print "***** time taken to converting dict objects to lists for user:" + userid + " = " + str(elapsed)

    starttime = time.time()
    apache_projects = '';
    index = 0;
    
    for project in asf_projects:
        if (index > 0):
            apache_projects = apache_projects + ", "
        apache_projects = apache_projects + project
        index = index + 1

    jsonresponse['tk_score'] = score
    jsonresponse['apache_projects'] = apache_projects
    jsonresponse['total_questions'] = total_questions
    jsonresponse['total_answers'] = total_answers
    jsonresponse['total_accepted_answers'] = total_accepted_answers
    jsonresponse['total_upvotes'] = total_upvotes
    jsonresponse['total_downvotes'] = total_downvotes
    jsonresponse['topic_questions'] = topic_questions_list
    jsonresponse['topic_answers'] = topic_answers_list
    jsonresponse['topic_accepted_answers'] = topic_accepted_answers_list
    jsonresponse['topic_question_upvotes'] = topic_question_upvotes_list
    jsonresponse['topic_question_downvotes'] = topic_question_downvotes_list
    jsonresponse['topic_answer_downvotes'] = topic_answer_downvotes_list
    jsonresponse['topic_answer_upvotes'] = topic_answer_upvotes_list
    jsonresponse['distinct_topic_info'] = distinct_tag_info_list
    jsonresponse['distinct_topic_vote'] = distinct_tag_vote_list
    jsonresponse['tag_percent_list'] = tag_percent_list
    jsonresponse['distinct_tag_question_list'] = distinct_tag_question_list
    jsonresponse['distinct_tag_answer_list'] = distinct_tag_answer_list
    jsonresponse['distinct_tag_acceptedanswer_list'] = distinct_tag_acceptedanswer_list
    jsonresponse['distinct_topic_list'] = distinct_topic_list
    jsonresponse['topic_percentile_list'] = topic_percentile_list

    elapsed = time.time() - starttime;
    print "***** time taken to assemble the response object for user:" + userid + " = " + str(elapsed)
   
    return jsonresponse

