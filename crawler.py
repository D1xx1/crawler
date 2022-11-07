import datetime
import random
import sqlite3
import requests
import bs4

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
            text = self.getTextOnly(soup)
            words = self.separateWords(text)
            for i in range(0,len(words)):
                try:
                    self.cur.execute(f"""INSERT INTO wordList(word, isFiltred) VALUES ('{words[i]}','0')""")
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
        print(newList)
        return newList

    def getTextOnly(self, soup):
        text = soup.get_text()
        return text

    def addLinkRef(self, urlFrom, urlTo, linkText):
        pass

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
            isFiltred INT
        )''')
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS wordLocation (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            fk_wordId BIGINT,
            fk_URLId BIGINT,
            location BIGINT
        )''')
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS linkBetweenURL (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            fk_FromURL_Id BIGINT,
            fk_ToURL_Id BIGINT
        )''')
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS linkWord (
            rowId INTEGER PRIMARY KEY AUTOINCREMENT,
            fk_wordId BIGINT,
            fk_linkId BIGINT
        )''')
        self.conn.commit()
        print('БД создана')


    def getEntryId(self, tableName, fileName, value=1):
        pass

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
                                hrefs.append(a['href'])
                                nextUrlSet.add(a['href'])
                                urlList = list(nextUrlSet)
                except IndexError:
                    continue
            nextUrlSet.clear()
        print('====== Completed ======')


if __name__ == '__main__':
    crawler = Crawler('database.db')
    crawler.initDB()
    urlList = ['https://habr.com/ru/post/694932/'] # Начальная ссылка
    crawler.crawl(urlList,4) # Точка входа паука.
