import pandas as pd
import wikipedia

from bs4 import BeautifulSoup

import os

WIKI_FILE_PATH = os.path.dirname(os.path.abspath(__file__))

wikipedia.set_lang("fr")


class WikiChannelDataManager:

    def __init__(self, filepath):
        self.channel_list = pd.read_excel(filepath)
        print(self.channel_list.head(2))
        self.channel_list = self.channel_list['CHANNEL_NAME']

    def get_summary(self, page):
            return page.summary

    def get_html(self, page):
            return page.html()

    def get_html_table(self, html):
        soup = BeautifulSoup(html, 'lxml')
        rows = soup.body.find('table').findAll("tr", attrs={'class':''})
        return rows

    def get_group(self, rows): 
        pass

    def get_channel_type(self, rows):
        for row in rows:
            if 'Statut' in row.text:
                print(row.find('div').text)
    
    def search_channels(self):
        for idx, channel in enumerate(self.channel_list[1:]):
            try:
                searches = wikipedia.search(channel)
                if len(searches) == 0:
                    print(f'Nothing found for {channel}')
                    continue
                page = wikipedia.page(searches[0], auto_suggest=False)
                print(idx, page.url)
            except wikipedia.exceptions.DisambiguationError:
                try:
                    searches = wikipedia.search(channel)
                    page = wikipedia.page(searches[0] + ' (chaîne de télévision)', auto_suggest=False)
                    print(idx, page.url)
                except wikipedia.exceptions.PageError:
                    print(f'Nothing found for {channel}')
            rows = self.get_html_table(self.get_html(page))
            self.get_channel_type(rows)
            break


        
            


if __name__ == '__main__':
    manager = WikiChannelDataManager(os.path.join(WIKI_FILE_PATH, '../../data/channels.xlsx'))
    manager.search_channels()