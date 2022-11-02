import pandas as pd
import wikipedia

from bs4 import BeautifulSoup

import json
import os
import re

WIKI_FILE_PATH = os.path.dirname(os.path.abspath(__file__))

wikipedia.set_lang("fr")


class WikiChannelDataManager:

    def __init__(self, filepath):
        self.channel_list = pd.read_excel(filepath)
        self.channel_list = self.channel_list['CHANNEL_NAME']

    def get_summary(self, page):
            return page.summary

    def get_html(self, page):
            return page.html()

    def get_html_summary_table(self, html):
        soup = BeautifulSoup(html, 'lxml')
        rows = soup.body.find('table').findAll("tr", attrs={'class':''})
        return rows

    def get_group(self, rows): 
        for row in rows:
            if 'Propriétaire' in row.text:
                return row.find('div').text.strip()

    def remove_parenthesis_substr(self, text):
        return re.sub("[\(\[].*?[\)\]]", "", text)

    def get_channel_type(self, rows):
        for row in rows:
            if 'Statut' in row.text:
                return self.remove_parenthesis_substr(row.find('div').text.strip()).split(' ')
    
    def search_channels(self):
        results = {}
        for idx, channel in enumerate(self.channel_list[0:10]):
            channel_name = channel.replace(' ', '_').lower()
            results[channel_name] = {}
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
            html = self.get_html(page)
            rows = self.get_html_summary_table(html)
            results[channel_name]['group'] = self.get_group(rows)
            results[channel_name]['type'] = self.get_channel_type(rows)
        return results


if __name__ == '__main__':
    manager = WikiChannelDataManager(os.path.join(WIKI_FILE_PATH, '../../data/channels.xlsx'))
    results = manager.search_channels()
    if not os.path.isdir(os.path.join(WIKI_FILE_PATH, '../../data/channels/')):
        os.mkdir(os.path.join(WIKI_FILE_PATH, '../../data/channels/'))
    with open(os.path.join(WIKI_FILE_PATH, '../../data/channels/results.json'), 'w', encoding='utf8') as jf:
        json.dump(results, jf, indent=2, ensure_ascii=False)