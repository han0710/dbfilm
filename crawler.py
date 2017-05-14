#-*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import logging
import time
import Controller as c
from db import r
import random
import signal
from pymongo import MongoClient

client=MongoClient('localhost',27017)
db=client.dbfilm
films=db.films

class dbfilmC(object):
	'''
	爬虫基类；
	kw接受参数：page_num
	'''
	def __init__(self,task_name,**kw):
		# 属性
		self._tName=task_name
		self._uKey='%s-urls'%task_name
		self._uKey_1='%s-urls-1'%task_name
		self._user_agent='Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36'
		if kw.get('page_num'):
			self._pageNum=kw.get('page_num')
	    
		# 方法
		self.add_urls()
		
	def add_urls(self):
		'''
		向任务池添加任务；
		1.如果任务池不存在，则初始化
		2.如果任务池存在，则添加
		'''
		if c.pool_empty(self._uKey_1):
			base_url='https://movie.douban.com/tag/%E4%B8%AD%E5%9B%BD%E5%A4%A7%E9%99%86'
			for i in range(self._pageNum):
				url='%s?start=%d&type=T'%(base_url,20*i)
				c.push(self._uKey_1,url)

	def _pageC_1(self):
		url=c.pop(self._uKey_1)
		print('请求url：%s'%url)
		headers={'user_agent':self._user_agent}
		page=requests.get(url,headers=headers)
		soup=BeautifulSoup(page.text,'html.parser')
		trs=soup.find_all('tr',{'class':'item'})
		for tr in trs:
			url=tr.find('a',{'class':'nbg'})['href']
			c.push(self._uKey,url)
				
	def _pageC(self):
		'''
		1.构造请求
		2.获得响应
		3.解析网页
		4.获取数据
			[2-4].1. 如果响应/解析/数据异常，则归还本任务
		5.归还代理
			5.1. 如果未使用代理，则跳过
		6.添加新链
			6.1. 如果无需动态添加任务链接，则跳过
		'''
		url=c.pop(self._uKey)
		print('请求url：%s'%url)
		headers={'user-agent':'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36'}
		try:
			page=requests.get(url,headers=headers)
			soup=BeautifulSoup(page.text,'html.parser')
			dict={'电影':soup.title.string}
			
			spans=soup.find('div',{'id':'info'}).find_all('span',{'class':'pl'})
			for span in spans:
				key=span.string
				nsb=span.find_next_sibling()
				if nsb.has_attr('class') and 'attrs' in nsb['class']:
					value=[]
					for i in nsb.find_all('a'):
						value.append(i.string)
				elif nsb.has_attr('property'):
					value=[]
					while True:
						value.append(nsb.string)
						nsb=nsb.find_next_sibling()
						if not nsb.has_attr('property'):
							break
				elif nsb.has_attr('href'):
					value=nsb['href']
				else:
					value=span.next_sibling
				dict[key]=value
			strong_ll=soup.find('strong',{'class':'ll'})
			dict['rating']=strong_ll.string
			dict['rating_url']='%s/collections'%url
			films.insert_one(dict)
			#c.push(self._tName,pickle.dumps(dict))
		except Exception as e:
			logging.exception(e)
			c.push(self._uKey,url)
				
	def start(self):
		while not c.pool_empty(self._uKey_1):
			self._pageC_1()
			time.sleep(random.uniform(10,15))
		while not c.pool_empty(self._uKey):
			self._pageC()
			time.sleep(random.uniform(10,15))
		print('任务已完成，退出。')
			
if __name__=='__main__':
	dbfilm=dbfilmC('dbfilm',page_num=103)
	dbfilm.start()

		