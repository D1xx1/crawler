import sqlite3
import datetime
import requests
import bs4
import random
import re

class Crawler:
    def __init__(self, fileName:str):
        self._conn = sqlite3.connect(fileName)
        self._cur = self._conn.cursor()
        

    def __del__(self):
        self._conn.commit()
        self._conn.close()

    def addIndex(self, text):
        if self.isIndexed==1:
            pass
        # вызов должен быть с текстом
        # 
        text = self.getTextOnly(text)
        words = [self.separateWords(text)]

        urlId = self.getEntryId("urllist","url")
        for i in range(words):
            pass
        

    def getTextOnly(self, text):
        pass

    def addLinkRef(self, urlFrom, urlTo, linkText):
        pass

    def crawl(self, urlList, maxDepth=1):
        try:
            for currDepth in range(0,maxDepth):
                
                for url in urlList:
                    html_doc = requests.get(url).text
                    soup = bs4.BeautifulSoup(html_doc, "html.parser")
                    a = []
                    hrefs = []
                    for a in soup.find_all('a', href=True):
                        if a['href'].startswith('http'):
                            hrefs.append(a['href'])
                            urlList.append(a['href'])
                            
                    print(urlList)
                currDepth=currDepth+1
                if currDepth == maxDepth:
                    break
        except Exception as error:
            print(error)
            pass
                # выделить ссылку 
                # добавить ссылку в список следующих на обход
                # извлечь из тэг <a> текст linkText
                # добавить в таблицу linkbeetwenurl БД ссылку с одной страницы на другую

                # self.addIndex(soup, url)
            pass
        pass
    pass

    def initDB(self):
        # cur = self.connection.cursor()
        self._cur.execute('''
        CREATE TABLE IF NOT EXISTS URLList (
            rowId BIGINT PRIMARY KEY,
            URL TEXT
        )''')
        self._cur.execute('''
        CREATE TABLE IF NOT EXISTS wordList (
            rowId BIGINT PRIMARY KEY,
            word TEXT,
            isFiltred INT
        )''')
        self._cur.execute('''
        CREATE TABLE IF NOT EXISTS wordLocation (
            rowId INT PRIMARY KEY,
            fk_wordId BIGINT,
            fk_URLId BIGINT,
            location BIGINT
        )''')
        self._cur.execute('''
        CREATE TABLE IF NOT EXISTS linkBetweenURL (
            rowId BIGINT PRIMARY KEY,
            fk_FromURL_Id BIGINT,
            fk_ToURL_Id BIGINT
        )''')
        self._cur.execute('''
        CREATE TABLE IF NOT EXISTS linkWord (
            rowId BIGINT PRIMARY KEY,
            fk_wordId BIGINT,
            fk_linkId BIGINT
        )''')
        self._conn.commit()
        print('БД создана')

    def getEntryId(self, tableName, fileName, value=1):
        pass

    def separateWords(self, text:str) -> str:
        text = text.split()
        return text

if __name__ == '__main__':
    crawler = Crawler('database.db')
    crawler.initDB()
    urlList = ['https://habr.com/ru/post/694932/']
    crawler.crawl(urlList)
