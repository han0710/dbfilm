#coding=utf-8

from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
from urlparse import urljoin

client=MongoClient('localhost',27017)
db=client.dbfilm
films=db.films

# initial url
start_url='https://movie.douban.com/tag/%E4%B8%AD%E5%9B%BD%E5%A4%A7%E9%99%86'

# crawl
def crawl(page_num=1):
	film_urls=[]
	for i in xrange(0,page_num):
		params={'start':'%s'%(20*i),'type':'T'}
		r=requests.get(start_url,params=params)
		soup=BeautifulSoup(r.text,'html.parser')
		
		for tag in soup.select('div.pl2 > a'):
			url=tag.attrs['href']
			film_urls.append(url)
	
	vec_keys=[u'导演',u'编剧',u'主演',u'类型']
	for url in film_urls:
		r=requests.get(url)
		soup=BeautifulSoup(r.text,'html.parser')
		film={}
		for key in vec_keys:
			film[key]=[]
		try:	
			#片名
			title=soup.find('span',property='v:itemreviewed')
			film[u'片名']=title.string
			
			#导演
			directors=soup.find_all('a',rel='v:directedBy')
			for dir in directors:
				film[u'导演'].append(dir.string)
			#编剧
			writers=directors[-1].find_next('span',class_='attrs').find_all('a')
			for wri in writers:
				film[u'编剧'].append(wri.string)
			#主演
			actors=soup.find_all('a',rel='v:starring')
			for act in actors:
				film[u'主演'].append(act.string)
			#类型
			types=soup.find_all('span',property='v:genre')
			for typ in types:
				film[u'类型'].append(typ.string)
			#上映日期
			release=soup.find('span',property='v:initialReleaseDate')
			film[u'上映日期']=release.string
			#片长
			runtime=soup.find('span',property='v:runtime')
			film[u'片长']=runtime.string
			
			film[u'评分链接']=url+'collections'
			film[u'影评链接']=url+'reviews'
		except:
			film={'url':url}
		
		films.insert_one(film)

if __name__=='__main__':
	crawl()
	
	
	
	
