#-*- coding:utf-8 -*-
from db import r
import threading
import functools
import sys

INT_OR_TERM=False
lock=threading.Lock()

def with_lock(func):
	@functools.wraps(func)
	def wrapper(*arg,**kw):
		lock.acquire()
		res=func(*arg,**kw)
		lock.release()
		if isinstance(res,tuple):
			return res[0],res[1]
		else:
			return res
	return wrapper
	
@with_lock	
def pop(key,ws=False):
	'''
	从任务池或代理池左侧获得第一个元素，并删除；
	'''
	tp=r.type(key).decode('ascii')
	if tp=='list':
		return r.lpop(key).decode('ascii')
	elif tp=='zset':
		if ws:
			ele,score=r.zrange(key,0,0,withscores=ws)[0]
			r.zrem(key,ele)
			return ele.decode('ascii'),score
		else:
			ele=r.zrange(key,0,0)[0]
			r.zrem(key,ele)
			return ele.decode('ascii')
	else:
		print('只接受列表（list）或有序集合（zset）!')

@with_lock
def push(key,value,score=None):
	'''
	向任务池右侧添加一个元素；或向代理池添加一个元素并自动排序；
	'''
	tp=r.type(key).decode('ascii')
	if tp=='list' or score is None:
		r.rpush(key,value)
	elif tp=='zset' or not score is None:
		r.zadd(key,value,score)

@with_lock
def pool_del(key):
	'''
	删除一个任务池或代理池；
	'''
	r.delete(key)

def copy_proxy(dest_key,key):
	'''
	复制一个代理池
	'''
	r.zunionstore(dest_key,{key:0,dest_key:1})
	
def pool_empty(key):
	'''
	检验代理池或任务池是否已空
	'''
	if r.exists(key):
		return False
	else:
		return True

def zpagination(key,start,end,ws=False):
	'''
	有序集合支持分页操作
	'''
	tp=r.type(key).decode('ascii')
	if tp=='zset':
		if start<(end-1):
			res=r.zrange(key,start,end-1,withscores=ws)
			return res
		else:
			print('索引错误，结束位置先于起始位置。')
	else:
		print('仅支持redis有序集合类型。')
		
def get_num(key):
	'''
	获得对象中元素个数
	'''
	tp=r.type(key).decode('ascii')
	if tp=='list':
		return r.llen(key) 
	elif tp=='zset':
		return r.zcard(key)
	else:
		print('仅支持redis列表及有序集合')

# def pool_swallow(key,thres):
	# '''
	# 检验代理池是否代理数过少
	# '''
	# if r.zcard(key)<thres:
		# return True
	# else:
		# return False

# def cprint(*args):
	# print(*args)
	# sys.stdout.flush()

