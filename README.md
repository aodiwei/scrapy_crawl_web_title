# scrapy_crawl_web_title 
use scrapy+ redis build distributed crawler to crawl large number of title of webs
# Features:
1. use redis creat task queue which all urls need to crawl in it
2. distributed crawler
3. fix encoding error with unread character(乱码), cause there are so many webs which have different encode, some are follow rule but some are not.
4. save crawl result in mysql

# import csv to redis
>> awk -F, 'NR > 1{ print " lpush", "\"task:url:queue\"", "\""$0"\"" }' url.csv | redis-cli --pipe

# how to run
 1. pip3 install -r requirements.txt
 2. run `scrapy crawl web_title`
