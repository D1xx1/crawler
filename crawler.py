import datetime
import random
import sqlite3
import requests
import bs4
import re

class Crawler:
    def __init__(self, fileName:str):
        self.conn = sqlite3.connect(fileName)
        self.cur = self.conn.cursor()
        

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def isIndexed(self, url) -> bool:
        self.cur.execute(f"SELECT * FROM urllist WHERE url == '{url}'")
        now = self.cur.fetchall()
        if now == []:
            return False
        else:
            return True

    def addIndex(self, soup, url):
        if self.isIndexed(url) == False:
            self.cur.execute(f"""INSERT INTO urllist(url) VALUES ('{url}')""")
            self.conn.commit()
            self.cur.execute(f"""SELECT rowId FROM urllist WHERE url == '{url}'""")
            nowUrlId = self.cur.fetchone()
            nowUrlId = re.findall('(\d+)', str(nowUrlId))
            text = self.getTextOnly(soup)
            words = self.separateWords(text)
            for i in range(0,len(words)):
                try:
                    self.cur.execute(f"""INSERT INTO wordList(word, isFiltred, urlId) VALUES ('{words[i]}','0','{int(nowUrlId[0])}')""")
                except Exception:
                    continue
            self.conn.commit()
            self.cur.execute(f"""SELECT rowId FROM wordList WHERE urlId == '{int(nowUrlId[0])}'""")
            newWordsId = self.cur.fetchall()
            newWordsId = re.findall('(\d+)', str(newWordsId))
            for i in range(len(newWordsId)):
                try:
                    self.cur.execute(f"""INSERT INTO wordLocation(fk_wordId, fk_URLId, location) VALUES ('{int(newWordsId[i])}','{int(nowUrlId[0])}','{i}')""")
                except Exception as error:
                    print(error)
                finally:
                    continue
            self.conn.commit()
            print('слова получены с',url)
        else:
            print('Уже есть в индексе.')
            pass

    def separateWords(self, text:str) -> list:
        newList = text.split()
        # print(newList)
        return newList

    def getTextOnly(self, soup) -> str:
        text = soup.get_text()
        return text

    def addLinkRef(self, urlFrom, urlTo):
        self.cur.execute(f"""SELECT rowId FROM urllist WHERE url == '{urlFrom}'""")
        urlFrom = self.cur.fetchone()
        urlFrom = re.findall('(\d+)',str(urlFrom))
        self.cur.execute(f"""INSERT INTO linkBetweenURL(fk_FromURL_Id, fk_ToURL_Id) VALUES ('{int(urlFrom[0])}','{urlTo}')""")
        self.conn.commit()

    def initDB(self):
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS urllist (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            url VARCHAR
        )''')
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS wordList (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            word VARCHAR,
            isFiltred INTEGER,
            urlId INTEGER
        )''')
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS wordLocation (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            fk_wordId INTEGER,
            fk_URLId INTEGER,
            location INTEGER
        )''')
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS linkBetweenURL (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            fk_FromURL_Id INTEGER,
            fk_ToURL_Id INTEGER
        )''')
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS linkWord (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            fk_wordId INTEGER,
            fk_linkId INTEGER
        )''')
        self.conn.commit()
        print('БД создана')


    def crawl(self, urlList:list, maxDepth:int):
        nextUrlSet = set()
        for currDepth in range(0, maxDepth):
            print('======= Глубина обхода',currDepth+1,'========')
            for i in range(0,len(urlList)):
                try:
                    url = urlList[i]
                    try:
                        html_doc = requests.get(url).text
                    except Exception as error:
                        print(error)
                        continue
                    soup = bs4.BeautifulSoup(html_doc, 'html.parser')
                    listUnwantedItems = ['scripts', 'style']
                    for script in soup.find_all(listUnwantedItems):
                        script.decompose()
                    self.addIndex(soup, url)
                    hrefs = list()
                    for a in soup.find_all('a', href=True):
                            if a['href'].startswith('http'):
                                if not a['href'].startswith('https://twitter'):
                                    hrefs.append(a['href'])
                                    nextUrlSet.add(a['href'])
                                    urlList = list(nextUrlSet)
                    self.addLinkRef(url,urlList[i])
                except IndexError:
                    continue
                
            nextUrlSet.clear()
        print('====== Completed ======')


if __name__ == '__main__':
    crawler = Crawler('database.db')
    crawler.initDB()
    urlList = ['https://habr.com/ru/post/694932/'] # Начальная ссылка
    crawler.crawl(urlList,4) # Точка входа паука.
