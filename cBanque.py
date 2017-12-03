# -*- coding: utf-8 -*- 
import os, time, logging, requests, csv
import psycopg2
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import *

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('log.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)


def get_db_credentials():
    global hostname_db
    global port_db
    global username_db
    global password_db
    global database_db
    
    cd = os.path.dirname(os.path.abspath('__file__'))
    db_credentials={}
    csvFile = os.path.join(cd, 'db_credentials.csv')
    with open(csvFile) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=';')
        for row in readCSV:
            db_credentials[row[0]]=(row[1])

    hostname_db = db_credentials['hostname_db']
    port_db = db_credentials['port_db']
    username_db = db_credentials['username_db']
    password_db = db_credentials['password_db']
    database_db = db_credentials['database_db']

get_db_credentials()

CONNECT_STRING = 'postgresql://' + username_db + ':' + password_db + '@' + hostname_db + ':' + port_db + '/' + database_db
engine = create_engine(CONNECT_STRING)

Base = declarative_base()



class Parse(Base):
    __tablename__ = 'parse'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    title = Column(String)
    left_info = Column(String)
    right_info = Column(String)
    
    def __init__(self, name, title, left_info, right_info):
        self.name = name
        self.title = title
        self.left_info = left_info
        self.right_info = right_info
    
    
def get_proxy():
    proxy_url = 'https://free-proxy-list.net/'
    r = requests.get(proxy_url)
    soup = BeautifulSoup(r.text)
    tbody = soup.find('tbody')
    
    urls = []
    
    for tr in tbody:
        td = tr.find_all('td')[:2]
        ip = td[0].text
        port = td[1].text
        url = ip + ':' + port
        urls.append(url)
        
    for proxy in urls:    
        url = 'http://' + proxy
        try:
            r = requests.get('http://facebook.com', proxies={'http': url})
            if r.status_code == 200:
                return url
        except:
            continue
            
proxy = get_proxy()              


def get_html(url):
    r = requests.get(url, proxies={'http': proxy})
    if r.status_code != 200:
        logger.info('Response not equal 200, response = {} '.format(r.status_code))
    return r.text


def parse(html):
    soup = BeautifulSoup(html)

    tables = soup.find_all('table', class_='orangetable orangeborder small')
    
    for name in soup.find_all('h1'):
        bank_names=name.text
        if not bank_names:
            logger.info('Not parsing "bank_names"')
    for table in tables:
        rows = table.find_all('tr')[1:]
        for row in rows:
            if len(row) == 1:
                for title in row.findAll('td', class_='intertitre'):
                    bank_titles=title.text
                    if not bank_titles:
                        logger.info('Not parsing "bank_titles"')
            if len(row) == 2:
                for left_info in row.find_all('td')[:1]:
                    bank_left_infos=left_info.text.replace('\u0092', "'")
                    if not bank_left_infos:
                        logger.info('Not parsing "bank_left_infos"')
                for right_info in row.find_all('td')[1:]:
                    bank_right_infos=right_info.text.replace('\u0080', '€')
                    if not bank_right_infos:
                        logger.info('Not parsing "bank_right_infos"')

                    try:
                        Session = sessionmaker(bind=engine)

                        Base.metadata.create_all(engine)
                        session = Session()

                        parse = Parse(bank_names, bank_titles, bank_left_infos, bank_right_infos)
                        session.add(parse)   
                        session.commit()
                        session.close()
                    except:
                        logger.info('Data not save into database')[:10]
                        
                    
                    
def main():    
    base_url = 'https://www.cbanque.com/tarif-bancaire/'
    banks = ['bforbank/', 'boursorama-banque/', 'elcl/', 'fortuneo/', 'hello-bank/', 
            'ing-direct/', 'monabanq/', 'orange-bank/', 'compte-soon/']
    for bank in banks:
        url = base_url + bank
        html = get_html(url)
        parse(html)
        

if __name__ == '__main__':
    main()