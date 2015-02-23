#coding=utf-8
import MySQLdb
from bs4 import BeautifulSoup
import json
import re
import time
from math import ceil
import logging
import threading
import Queue
import ConfigParser

from util import get_content


class UpdateOneTopic(threading.Thread):
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

		self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
		self.cursor = self.db.cursor()
		
	def run(self):
		while not self.queue.empty():
			t = self.queue.get()
			link_id = t[0]
			count_id = t[1]
			self.find_new_question_by_topic(link_id,count_id)

	def find_question_by_link(self,topic_url,count_id):
		content = get_content(topic_url,count_id)

		if content == "FAIL":
			return 0

		soup = BeautifulSoup(content)

		questions = soup.findAll('a',attrs={'class':'question_link'})

		i = 0
		p_str = 'INSERT IGNORE INTO QUESTION (NAME, LINK_ID, FOCUS, ANSWER, LAST_VISIT, ADD_TIME, TOP_ANSWER_NUMBER) VALUES (%s, %s, %s, %s, %s, %s, %s)'
		anser_list = []
		time_now = int(time.time())

		for question in questions:
			tem_text = question.get_text()
			tem_id = question.get('href')
			tem_id = tem_id.replace('/question/','')

			anser_list = anser_list + [(tem_text, int(tem_id), 0, 0, 0, time_now, 0)]

		self.cursor.executemany(p_str,anser_list)

		return self.cursor.rowcount

	def find_new_question_by_topic(self,link_id,count_id):
		new_question_amount_total = 0
		for i in range(1,7):
			topic_url = 'http://www.zhihu.com/topic/' + link_id + '/questions?page=' + str(i)
			new_question_amount_one_page = self.find_question_by_link(topic_url,count_id)
			new_question_amount_total = new_question_amount_total + new_question_amount_one_page

			if new_question_amount_one_page <= 2:
				break
		
		if count_id % 2 == 0:
			print str(count_id) + " , " + self.getName() + " Finshed TOPIC " + link_id + ", page " + str(i) + " ; Add " + str(new_question_amount_total) + " questions."

		time_now = int(time.time())
		sql = "UPDATE TOPIC SET LAST_VISIT = %s WHERE LINK_ID = %s"
		self.cursor.execute(sql,(time_now,link_id))

class UpdateTopics:
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

		self.topic_thread_amount = int(cf.get("topic_thread_amount","topic_thread_amount"))

		self.db = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, charset=charset, use_unicode=use_unicode)
		self.cursor = self.db.cursor()

	def run(self):
		time_now = int(time.time())
		before_last_vist_time = time_now - 10

		queue = Queue.Queue()
		threads = []

		i = 0

		sql = "SELECT LINK_ID FROM TOPIC WHERE LAST_VISIT < %s ORDER BY LAST_VISIT"
		self.cursor.execute(sql, (before_last_vist_time,))
		results = self.cursor.fetchall()
		
		for row in results:
			link_id = str(row[0])

			queue.put([link_id, i])
			i = i + 1

		for i in range(self.topic_thread_amount):
			threads.append(UpdateOneTopic(queue))

		for i in range(self.topic_thread_amount):
			threads[i].start()

		for i in range(self.topic_thread_amount):
			threads[i].join()

		self.db.close()

		print 'All task done'

a = UpdateTopics()
a.run()
