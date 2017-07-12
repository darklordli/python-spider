#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sqlite3
import re
import time,threading
import urllib2,os
import hashlib
import MySQLdb
import datetime
from multiprocessing import Pool
_db_host = '127.0.0.1'
_db_user = 'root'
_db_pwd = '123456'
_db_name = 'spider'
_db_table = 'spider_data2'
_down_dir = '/roobo/webserver/download2'
def read_image_info(read_num):
    db = MySQLdb.connect(_db_host,_db_user,_db_pwd,_db_name,charset='utf8')
    cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    sql = "select * from %s where down_status=0 limit %d" % (_db_table, read_num)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    db.close()
    return rows
def down_image(image_info):
    db = MySQLdb.connect(_db_host,_db_user,_db_pwd,_db_name,charset='utf8')
    cursor = db.cursor(cursorclass = MySQLdb.cursors.DictCursor)
    sql = "select * from %s where keyword='%s' " % ('keywords', image_info['keyword'])
    cursor.execute(sql)
    row = cursor.fetchone()
    image_url = image_info['img_link']
    save_path_dir = _down_dir + '/' + str(row['id']) + '/'
    image_ext = image_url[image_url.rindex('.')+1:]
    if image_ext == '':
        image_ext = 'jpg'
    image_name = hashlib.md5(image_url).hexdigest() + '.' + image_ext
    save_path = save_path_dir + image_name
    down_status = 1
    if os.path.exists(save_path_dir) == False:
        os.makedirs(save_path_dir)
    try:
        if os.path.exists(save_path) == False:
            request = urllib2.Request(image_url)
            request.add_header('User-Agent','Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36')
            response = urllib2.urlopen(request, timeout=10)
            data = response.read()
            f = open(save_path, 'wb')
            f.write(data)
            f.close()
            print image_url,' done!'
    except:
        down_status = -1
        save_path = ''
        print image_url, ' failed!'
    now = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    sql = "update %s set down_status=%d , download_at='%s', local_path='%s' where id=%d" % (_db_table, down_status, now, save_path, int(image_info['id']))
    if down_status == 1 :
        sql2 = "update %s set download_count=download_count+1 where id=%d limit 1" % ('keywords', int(row['id']))
        cursor.execute(sql2)
    cursor.execute(sql)
    db.commit()
    cursor.close()
    db.close()
    return
if __name__=='__main__':
    start_t = int(time.time())
    read_num = 100
    process_num = 5
    image_list = read_image_info(read_num)
    while image_list :
        pool = Pool(process_num)
        for key,image in enumerate(image_list):
            pool.apply_async(down_image,args=(image,))
        pool.close()
        pool.join()
        image_list = read_image_info(read_num)
    print 'all done'
    print int(time.time()) - start_t