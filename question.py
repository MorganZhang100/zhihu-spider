#coding=utf-8
import MySQLdb
import urllib2
from bs4 import BeautifulSoup
import json
import re
import time
from math import ceil
import logging
import gzip
import StringIO
import threading
import Queue
import ConfigParser


def get_content(toUrl,count):
	""" Return the content of given url

		Args:
			toUrl: aim url
			count: index of this connect

		Return:
			content if success
			'Fail' if fail
	"""

	cf = ConfigParser.ConfigParser()
	cf.read("config.ini")
	cookie = cf.get("cookie", "cookie")

	headers = {
	    'Cookie': cookie,
		'Host':'www.zhihu.com',
		'Referer':'http://www.zhihu.com/',
		'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
		'Accept-Encoding':'gzip'
	}

	req = urllib2.Request(
	    url = toUrl,
	    headers = headers
	)

	try:
		opener = urllib2.build_opener(urllib2.ProxyHandler())
		urllib2.install_opener(opener)

		page = urllib2.urlopen(req,timeout = 15)

		headers = page.info()
		content = page.read()
	except Exception,e:
		if count % 1 == 0:
			print str(count) + ", Error: " + str(e) + " URL: " + toUrl
		return "FAIL"

	if page.info().get('Content-Encoding') == 'gzip':
		data = StringIO.StringIO(content)
		gz = gzip.GzipFile(fileobj=data)
		content = gz.read()
		gz.close()

	return content


class updateOneQuestion(threading.Thread):
	def __init__(self,queue):
		self.queue = queue
		threading.Thread.__init__(self)

		cf = ConfigParser.ConfigParser()
		cf.read("config.ini")
	    
		host = cf.get("db", "host")
		port = int(cf.get("db", "port"))
		user = cf.get("db", "user")
		passwd = cf.get("db", "passwd")
		db_name = cf.get("db", "db")
		charset = cf.get("db", "charset")
		use_unicode = cf.get("db", "use_unicode")

		self.question_thread_amount = int(cf.get("question_thread_amount","question_thread_amount"))

		self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
		self.cursor = self.db.cursor()
		
	def run(self):
		while not self.queue.empty():
			parameters = self.queue.get()
			link_id = parameters[0]
			count_id = parameters[1]
			self.update(link_id,count_id)

	def update(self,link_id,count_id):
		time_now = int(time.time())
		questionUrl = 'http://www.zhihu.com/question/' + link_id

		content = get_content(questionUrl,count_id)
		if content == "FAIL":
			sql = "UPDATE QUESTION SET LAST_VISIT = %s WHERE LINK_ID = %s"
			self.cursor.execute(sql,(time_now,link_id))
			return

		soup = BeautifulSoup(content)

		questions = soup.find('div',attrs={'class':'zg-gray-normal'})

		# Find out how many people focus this question.
		if questions == None:
			return
		else:
			focus_amount = questions.get_text().replace('\n','')
			focus_amount = focus_amount.replace(u'人关注该问题','')
			focus_amount = focus_amount.replace(u'关注','')

			if focus_amount == u'问题还没有':
				focus_amount = u'0'

		focus_amount = focus_amount.replace(u'问题','')

		if focus_amount == u'\\xe8\\xbf\\x98\\xe6\\xb2\\xa1\\xe6\\x9c\\x89':  # This is a special case.
			return

		# Find out how many people answered this question.
		answer_amount = soup.find('h3',attrs={'id':'zh-question-answer-num'})
		if answer_amount != None:
			answer_amount = answer_amount.get_text().replace(u' 个回答','')
		else:
			answer_amount = soup.find('div',attrs={'class':'zm-item-answer'})
			if answer_amount != None:
				answer_amount = u'1'
			else:
				answer_amount = u'0'

		# Find out the top answer's vote amount.
		top_answer = soup.findAll('span',attrs={'class':'count'})
		if top_answer == []:
			top_answer_votes = 0
		else:
			top_answer_votes = 0
			for t in top_answer:
				t = t.get_text()
				t = t.replace('K','000')
				t = int(t)
				if t > top_answer_votes:
					top_answer_votes = t

		# print it to check if everything is good.
		if count_id % 1 == 0:
			print str(count_id) + " , " + self.getName() + " Update QUESTION set FOCUS = " + focus_amount + " , ANSWER = " + answer_amount + ", LAST_VISIT = " + str(time_now) + ", TOP_ANSWER_NUMBER = " + str(top_answer_votes) + " where LINK_ID = " + link_id
		#print str(count_id) + " , " + self.getName() + " Update QUESTION set FOCUS = " + focus_amount + " , ANSWER = " + answer_amount + ", LAST_VISIT = " + str(time_now) + ", TOP_ANSWER_NUMBER = " + str(top_answer_votes) + " where LINK_ID = " + link_id
		
		# Update this question
		sql = "UPDATE QUESTION SET FOCUS = %s , ANSWER = %s, LAST_VISIT = %s, TOP_ANSWER_NUMBER = %s WHERE LINK_ID = %s"
		self.cursor.execute(sql,(focus_amount,answer_amount,time_now,top_answer_votes,link_id))

		# Find out the topics related to this question
		topics = soup.findAll('a',attrs={'class':'zm-item-tag'})
		if questions != None:
			sql_str = "INSERT IGNORE INTO TOPIC (NAME, LAST_VISIT, LINK_ID, ADD_TIME) VALUES (%s, %s, %s, %s)"
			topicList = []
			for topic in topics:
				topicName = topic.get_text().replace('\n','')
				topicUrl = topic.get('href').replace('/topic/','')
				#sql_str = sql_str + "('" + topicName + "',0," + topicUrl + "," + str(time_now) + "),"
				topicList = topicList + [(topicName, 0, topicUrl, time_now)]
			
			self.cursor.executemany(sql_str,topicList)


class UpdateQuestions:
	def __init__(self):
		cf = ConfigParser.ConfigParser()
		cf.read("config.ini")
	    
		host = cf.get("db", "host")
		port = int(cf.get("db", "port"))
		user = cf.get("db", "user")
		passwd = cf.get("db", "passwd")
		db_name = cf.get("db", "db")
		charset = cf.get("db", "charset")
		use_unicode = cf.get("db", "use_unicode")

		self.question_thread_amount = int(cf.get("question_thread_amount","question_thread_amount"))

		db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
		self.cursor = db.cursor()

	def run(self):
		queue = Queue.Queue()
		threads = []

		time_now = int(time.time())
		before_last_visit_time = time_now - 12*3600
		after_add_time = time_now - 24*3600*14

		sql = "SELECT LINK_ID from QUESTION WHERE LAST_VISIT < %s AND ADD_TIME > %s AND ANSWER < 8 AND TOP_ANSWER_NUMBER < 50 ORDER BY LAST_VISIT"
		self.cursor.execute(sql,(before_last_visit_time,after_add_time))
		results = self.cursor.fetchall()
		
		i = 0
		
		for row in results:
			link_id = str(row[0])

			queue.put([link_id, i])
			i = i + 1

		thread_amount = self.question_thread_amount

		for i in range(thread_amount):
			threads.append(updateOneQuestion(queue))

		for i in range(thread_amount):
			threads[i].start()

		for i in range(thread_amount):
			threads[i].join()

		db.close()

		print 'All task done'


if __name__ == '__main__':
	question_spider = UpdateQuestions()
	question_spider.run()