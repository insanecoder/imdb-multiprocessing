import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import multiprocessing
from multiprocessing import Process
import math

def getIMDBContentByKeyword(keyword):
	url= f'https://www.imdb.com/find?q={keyword}&s=tt&exact=true&ref_=fn_tt_ex'
	req= requests.get(url)
	data = req.text
	return data

def getInterestingUrls(data):
	soup = BeautifulSoup(data,features="lxml")
	table= soup.find('table',class_="findList")
	rows= table.find_all('tr')
	# uriList= {r.find('td',class_='result_text').text : r.find('td',class_='result_text').find('a')['href'] for r in rows }
	uriList= [ r.find('td',class_='result_text').find('a')['href'] for r in rows ]
	return uriList

def parseTitleUri(uri):
	url=f'https://www.imdb.com{uri}'
	req= requests.get(url)
	data = req.text
	soup = BeautifulSoup(data,features="lxml")
	summary= soup.find('div',class_='summary_text').text if soup.find('div',class_='summary_text') else ''
	rating= soup.find('div', class_='imdbRating').find('div',class_='ratingValue').text if soup.find('div', class_='imdbRating') else ''
	title= soup.find('div', class_='title_wrapper').text
	time.sleep(1) # just because we are good people scrapping. We don't want to burden IMDB server
	return {'summary':summary, 'rating':rating, 'titleText':title}

def getTitleInfoByUrl(uriList):
	resultDict={}
	for uri in uriList:
		titleID= uri.split('/')[2]
		resultDict[titleID]= parseTitleUri(uri)
	return resultDict


def getUriList(keyword):
	rawData= getIMDBContentByKeyword(keyword)
	uriList= getInterestingUrls(rawData)
	return uriList

def getEqualPartitionedList(num_processes,uriList):
	totalUris= len(uriList)
	batchSize= math.ceil(totalUris/num_processes)
	partitionedList= []
	for i in range(1,num_processes):
		partitionedList.append(uriList[batchSize*(i-1):batchSize*i])

	# adding the remaining elements as the last batch
	partitionedList.append(uriList[batchSize*(num_processes-1):])
	return partitionedList


def main(keyword,num_processes):
	uriList= getUriList(keyword)
	startTime = datetime.now()
	processList=[];partitionedList= getEqualPartitionedList(num_processes,uriList)

	for i,pList in enumerate(partitionedList):
		# 0 to 7 in first process;8 15 in second and so on......
		
		p= Process( target=getTitleInfoByUrl, args=(pList,) )
		processList.append(p)
		p.start()

	for p in processList:
		p.join()

	endTime = datetime.now()

	print(f"Time for running {num_processes} with keyword {keyword} is {endTime-startTime} seconds")

if __name__=='__main__':
	keyword= "space"
	num_processes= 8*multiprocessing.cpu_count()
	main(keyword,num_processes)