# -*- coding: utf-8 -*-
import re
from urllib.parse import urlparse

import scrapy
from bs4 import BeautifulSoup
import chardet

import tool
from crawl_web_title.items import CrawlWebTitleItem

redis = tool.RedisManage()
key = 'task:url:queue'

# logger = tool.get_logger()

# 乱码字符
UNREAD_PATTERN = re.compile(
    r'[ãäþíøçêðéúîöïõæÍµ¼»Ð±ÆÊÏ¾×¹¡¿'
    r'É½Ö²«ÓÒ¶Ì´ÈÕ¸ÅÔËÂÃÑ³º¨Ä¥ÀÎâ£Á¢ÇÛªó¯©¤Øû¬§Ýñ¦áèì®ù÷Úß'
    r'ëýåÞôòÙàüÜĞşビǱϺ˾Զ�涓浼洟氬瀛娑澶鏂撴夊槦宸栨嗗鍜渷鍏楁鐨樼腑夐鐩湁绱'
    r'畨闆姹瀹檺楅犺鍦暟堥眿婄骞崡甯囧嶅鍩绉窞獟緳鍗鍘湴垮叏忓徃洯鑻ユ灏婢'
    r'炶拌ㄨ埅傚戦鏃槻鏁姤偛鐎樿淇榛▼鍥婚浣氶洓槼ョ犲鏈棬綉瀵壒欒熺圭浗鎬鐜悆眹缃戠珯鏉冨寮埗浜搧璁よ瘉嚎娌冲︾溅闂埛㈣鎶ヤ琛屾儏'
    r'环T藉ㄦah杞‘°喘'
    r'\ue18d\ue179\ue0ff\ue11f\ue1d7\ue100\ue15e\u07b9\ue18dc\xad]')

IM_UNREAD_PATTERN = re.compile(r'[�\ue18d\ue179\ue0ff\ue11f涓]')  # 大概率乱码
EN_W_PATTERN = re.compile(r'[A-Za-z0-9]')
# 调度队列限制
MAX_SCHEDULER_QUEUE = 60
MAX_INPROGRESS_QUEUE = 50

# 每次从redis获取任务数量
YIELD_NUM = 2


class WebTitleSpider(scrapy.Spider):
    name = 'web_title'
    # allowed_domains = ['*']
    # start_urls = [redis.get_task(key)]

    start_urls = ['http://bj.99.com.cn', 'http://www.udg.com.cn', 'http://foruto.com', 'http://bbs.18888.com',
                  'http://www.china-fpa.org', 'http://www.dali.gov.cn', 'http://www.hwai.edu.tw', 'http://www.icshp.org',
                  'http://www.prulife.com.tw', 'http://jwc.swpu.edu.cn']

    def parse(self, response):
        """
        
        :param response: 
        :return: 
        """
        try:
            content = response.body
            content = content.strip(b'\xef\xbb\xbf').strip(b'\xc3\xaf\xc2\xbb\xc2\xbf')
            # if 'utf' not in response.encoding.lower():
            content = content.decode(response.encoding, 'replace')

            soup = BeautifulSoup(content, 'html.parser')
            tg = soup.find('title')
            if tg:
                title = tg.text.strip()
                en_w = EN_W_PATTERN.findall(title)
                len_t = len(title) - len(en_w)
                if len_t == 0:
                    title = 'NO TITLE'
                else:
                    unread_im = IM_UNREAD_PATTERN.findall(title)  # 大概率乱码
                    unread = UNREAD_PATTERN.findall(title)
                    if len(unread_im) > 1 or len(unread) / len_t > 0.6:
                        self.logger.info('{} title {} has unread chats'.format(response.url, title))
                        flag = False
                        if response.encoding.upper() != 'GB18030':
                            content = response.body.decode('GB18030', 'replace')
                            soup = BeautifulSoup(content, 'html.parser')
                            tg = soup.find('title')
                            title = tg.text.strip()
                            self.logger.info('{} decode GB18030 title {}'.format(response.url, title))
                            unread_im = IM_UNREAD_PATTERN.findall(title)  # 大概率乱码
                            unread = UNREAD_PATTERN.findall(title)
                            flag = True

                        if not flag or len(unread_im) > 1 or len(unread) / len_t > 0.6:
                            ret = chardet.detect(response.body)
                            content = response.body.decode(ret['encoding'], 'replace')
                            soup = BeautifulSoup(content, 'html.parser')
                            tg = soup.find('title')
                            title = tg.text.strip()
                            self.logger.info('{} finally decode {} title {}'.format(response.url, ret['encoding'], title))
            else:
                # logger.warning('{} no title'.format(response.url))
                title = 'NO TITLE'
            # ulr_obj = urlparse(response.url)  # 可能已经被跳转
            ulr_obj = urlparse(response.request.url)  # 原始url
            netloc = ulr_obj.netloc
            item = CrawlWebTitleItem()
            item['domain'] = netloc
            item['title'] = title
            self.logger.info('crawled domain {} title {}'.format(item['domain'], title))
            yield item
        except Exception as e:
            self.logger.error('crawled domain {} error {}'.format(response.url, e))

        sched_queue_len = len(self.crawler.engine.slot.scheduler)
        inprocess_queue_len = len(self.crawler.engine.slot.inprogress)

        if sched_queue_len < MAX_SCHEDULER_QUEUE and inprocess_queue_len < MAX_INPROGRESS_QUEUE:
            for _ in range(YIELD_NUM):
                url = redis.get_task(key)
                if url:
                    yield scrapy.Request(url=url, callback=self.parse, errback=self.parse_error)
        else:
            self.logger.info('scheduler {}, inprocess {} do not yield task'.format(sched_queue_len, inprocess_queue_len))

    def parse_error(self, response):
        """
        
        :param response: 
        :return: 
        """
        self.logger.error('error process {}'.format(response.request.meta['download_slot']))
        sched_queue_len = len(self.crawler.engine.slot.scheduler)
        inprocess_queue_len = len(self.crawler.engine.slot.inprogress)
        if sched_queue_len < MAX_SCHEDULER_QUEUE and inprocess_queue_len < MAX_INPROGRESS_QUEUE:
            for _ in range(YIELD_NUM):
                url = redis.get_task(key)
                if url:
                    yield scrapy.Request(url=url, callback=self.parse, errback=self.parse_error)
        else:
            self.logger.info('error process scheduler {}, inprocess {} do not yield task'.format(sched_queue_len, inprocess_queue_len))
