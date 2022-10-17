import sqlite3
import datetime
import requests
import bs4
import random
import re

class Crawler:
    def __init__(self, fileName):
        self.connection = sqlite3.connect(fileName)
        pass

    def __del__(self):
        self.connection.commit()
        self.connection.close()

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
        for currDepth in range(0,maxDepth):
            for url in urlList:
                html_doc = requests.get(url).text
                soup = bs4.BeautifulSoup(html_doc, "html.parser")
                # получить список тэгов <a> с текущей страницы
                # обработать каждый тэг <a>
                # проверить наличие атрибута 'href'
                # убрать пустые ссылки, вырезать якоря из ссылок, и т.д.
                # выделить ссылку 
                # добавить ссылку в список следующих на обход
                # извлечь из тэг <a> текст linkText
                # добавить в таблицу linkbeetwenurl БД ссылку с одной страницы на другую

                self.addIndex(soup, url)
            pass
        pass
    pass

    def initDB(self):
        pass

    def getEntryId(self, tableName, fileName, value=1):
        pass

    def separateWords(self, text:str) -> str:
        text = text.split()
        return text

        