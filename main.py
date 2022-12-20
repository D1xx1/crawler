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
        self.cur.execute(f"SELECT * FROM urllist WHERE url = '{url}'")
        now = self.cur.fetchall()
        if now == []:
            return False
        else:
            return True

    def addIndex(self, soup, url):
        if self.isIndexed(url) == False:
            self.cur.execute(f"""INSERT INTO urllist(url) VALUES ('{url}')""")
            self.conn.commit()
            self.cur.execute(f"""SELECT rowId FROM urllist WHERE url = '{url}'""")
            nowUrlId = self.cur.fetchone()
            nowUrlId = re.findall('(\d+)', str(nowUrlId))
            text = self.getTextOnly(soup)
            uniqueWords = self.separateWords(text, parameter=1)
            allWords = self.separateWords(text, parameter=2)

            '''Добавление слов в wordList'''
            for i in range(0,len(uniqueWords)):
                try:
                    # Вставка уникальных слов в БД
                    self.cur.execute(f"""INSERT INTO wordList(word, isFiltred, urlId) VALUES ('{str(uniqueWords[i])}','0','{int(nowUrlId[0])}')""")
                except Exception as error:
                    # print('ERROR_SECTION_ONE: '+str(error))
                    continue
            self.conn.commit()

            '''Добавление ID в wordLocation'''
            for i in range(len(allWords)):
                try:
                    self.cur.execute(f"""SELECT rowId FROM wordList WHERE word = '{str(allWords[i])}'""")
                    wordId = self.cur.fetchone()
                    self.cur.execute(f"""INSERT INTO wordLocation(fk_wordId, fk_URLId, location) VALUES ('{int(wordId[0])}','{int(nowUrlId[0])}','{i}')""")
                except Exception:
                    continue

            self.conn.commit()
        #     # print('слова получены с',url)
        # else:
        #     # print('Уже есть в индексе.')
        #     pass

    def separateWords(self, text:str, parameter:int) -> list:
        allWords = text.split()
        uniqueWords = set()
        for i in range(len(allWords)):
            uniqueWords.add(allWords[i])
        if parameter == 1:
            return list(uniqueWords)
        if parameter == 2:
            return list(allWords)
        
    def getTextOnly(self, soup) -> str:
        text = soup.get_text()
        return text

    def addLinkRef(self, urlFrom, urlTo):
        self.cur.execute(f"""SELECT rowId FROM urllist WHERE url = '{str(urlFrom)}'""")
        idFrom = self.cur.fetchone()
        for i in range(len(urlTo)):
            self.cur.execute(f"""SELECT rowId FROM urllist WHERE url = '{str(urlTo[i])}'""")
            idTo = self.cur.fetchone()
            try:
                if idTo is not None:
                    if idFrom != idTo:
                        self.cur.execute(f"""INSERT INTO linkBetweenURL(fk_FromURL_Id, fk_ToURL_Id) VALUES ('{idFrom[0]}','{idTo[0]}')""")
                        print(f'idFrom: {idFrom}, idTo: {idTo}')
            except Exception as error:
                print(error)
            finally:
                continue
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


    def crawl(self, urlList:list, maxDepth:int, mode:int):
        nextUrlSet = set()
        if mode == 2:
            self.cur.execute("""DROP TABLE IF EXISTS linkBetweenURL""")
            self.conn.commit()
            print('Таблица linkBetweenURL сброшена.')
            quit()
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
                    if mode == 0:
                        self.addIndex(soup, url)
                    hrefs = list()
                    for a in soup.find_all('a', href=True):
                            if a['href'].startswith('http'):
                                # if not a['href'].startswith('https://twitter'):
                                    hrefs.append(a['href'])
                                    nextUrlSet.add(a['href'])
                                    
                    
                    if mode == 1:
                        self.addLinkRef(url, urlList)
                except IndexError:
                    continue
            self.cur.execute('''SELECT rowId FROM urllist''')
            self.counter(currDepth)
            urlList = list(nextUrlSet)
            nextUrlSet.clear()
            
        print('====== Completed ======')

    def counter(self, currDepth):
        self.cur.execute('''SELECT rowId FROM urllist''')
        Urls = self.cur.fetchall()
        Urls = len(Urls)
        self.cur.execute('''SELECT rowId FROM wordList''')
        Words = self.cur.fetchall()
        Words = len(Words)
        print(f'На уровне {currDepth+1} количество ссылок - {Urls}; количество слов - {Words}.')



if __name__ == '__main__':
    crawler = Crawler('newdatabase1.db')
    crawler.initDB()
    urlList = ['https://habr.com/ru/post/694932/']
    # urlList = ['https://habr.com/ru/post/694932/','https://dzen.ru/a/YB5JMdlqGlC4sg5_','https://www.rbc.ru/rbcfreenews/63170ad29a7947a4bf1c302a'] # Начальная ссылка
    crawler.crawl(urlList, maxDepth=3, mode=0) # Точка входа паука.