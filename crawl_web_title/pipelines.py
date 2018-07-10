# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging
import tool


class CrawlWebTitlePipeline(object):
    def __init__(self):
        self.logger = logging.getLogger()
        self.mysql = tool.MySqlManage(self.logger, long_conn=True)

    def process_item(self, item, spider):

        sql = '''
               REPLACE INTO tab_domain_title_11w (domain, title)
                VALUES (%s, %s) 
           '''
        self.mysql.execute(sql, (item["domain"], item["title"]))

        return item
