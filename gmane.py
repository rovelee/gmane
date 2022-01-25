import re
import sqlite3
import ssl
import time
from urllib.request import urlopen
from gtools import parsemaildate

# 数据测试网站域名
baseurl = 'http://mbox.dr-chuck.net/sakai.devel/'

# 无视ssl认证错误
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# 连接数据库，如果不存在则在目录下创建一个
conn = sqlite3.connect('content.sqlite')
cur = conn.cursor()
# 如果表Message不存在则创建表
sql_create = '''CREATE TABLE IF NOT EXISTS Messages
    (id INTEGER UNIQUE, email TEXT, sent_at TEXT,
     subject TEXT, headers TEXT, body TEXT)'''
cur.execute(sql_create)
# 查询要开始爬取的初始id
sql_query = 'SELECT max(id) FROM Messages'
cur.execute(sql_query)
try:
    row = cur.fetchone()
    if row is not None:
        start = row[0]
except Exception as e:
    print('Database select start id false with:', e)
if start is None:
    start = 0

many = 0
fail = 0
count = 0
# 添加数据
while True:
    # 如果爬取数据错误大于五则退出循环
    if fail > 5:
        break
    # 输入要爬取的数据数量
    if many < 1:
        sval = input('How many messages:')
        if len(sval) < 1:
            break
        try:
            many = int(sval)
        except:
            print('Type number!')
            continue

    # 开始爬取数据
    many -= 1
    start += 1
    url = baseurl + str(start) + '/' + str(start+1)
    # 获取页面内容
    text = 'None'
    try:
        # 打开超过30秒超时
        document = urlopen(url, None, 30, context=ctx)
        text = document.read().decode()
    # 处理各种错误：
        # 页面代码不等于200，意味着连接错误
        if document.getcode() != 200:
            print("Error code=", document.getcode(), url)
            break
        # 使用Ctrl+c退出
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
        # 其他异常
    except Exception as e:
        print("Unable to retrieve or parse page", url)
        print("Error", e)
        fail = fail + 1
        continue
        # 如果text不是以From开头，则数据内容异常
    if not text.startswith('From'):
        print(text)
        print("Did not find From ")
        fail = fail + 1
        if fail > 5: break
        continue

    # 找到head和body的位置
    pos = text.find("\n\n")
    if pos > 0:
        header = text[:pos]
        body = text[pos+2:]
    else:
        # 数据内容异常
        print(text)
        print("Could not find break between headers and body")
        fail += 1
        continue

    # 开始处理数据
    count += 1
    # 使用正则查找email、sent_at、subject的值
    # From: "Glenn R. Golden" <ggolden@umich.edu>
    emails = re.findall('From: .* <(.+@.+)>', header)
    if len(emails) == 1:
        email = emails[0]
        email = email.strip().lower()
    else:
        emails = re.findall('From: .* (.+@.+) ', header)
        if len(emails) == 1:
            email = emails[0]
            email = email.strip().lower()
    date = None
    y = re.findall('Date: .*, (.*)', header)
    if len(y) == 1:
        tdate = y[0]
        tdate = tdate[:26]
        try:
            sent_at = parsemaildate(tdate)
        except:
            print(text)
            print("Parse fail", tdate)
            fail = fail + 1
            if fail > 5: break
            continue

    subject = None
    z = re.findall('Subject: (.*)', header)
    if len(z) == 1: subject = z[0].strip().lower();

    # Reset the fail counter
    fail = 0
    print("   ",start, email, sent_at, subject)
    cur.execute('''INSERT OR IGNORE INTO Messages (id, email, sent_at, subject, headers, body)
            VALUES ( ?, ?, ?, ?, ?, ? )''', (start, email, sent_at, subject, header, body))
    if count % 50 == 0: conn.commit()
    if count % 100 == 0: time.sleep(1)

conn.commit()
cur.close()
