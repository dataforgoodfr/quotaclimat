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
            """
            Return wiki page summary as str
            """
            return page.summary

    def get_html(self, page):
            """
            Get page html as str
            """
            return page.html()

    def get_html_summary_table(self, html):
        """
        Gets the first table in the page (usually the summary table) and returns the rows
        """
        soup = BeautifulSoup(html, 'lxml')
        try:
            rows = soup.body.find('table').findAll("tr", attrs={'class':''})
        except AttributeError:
            raise TypeError
        return rows

    def get_group(self, rows): 
        """
        Iterates rows to look for 'Propriétaire'
        """
        for row in rows:
            if 'Propriétaire' in row.text:
                try:
                    return row.find('div').text.strip()
                except AttributeError:
                    return None

    def remove_parenthesis_substr(self, text):
        """
        Removes substring in parenthesis
        """
        return re.sub("[\(\[].*?[\)\]]", "", text)

    def get_channel_type(self, rows):
        """
        Iterates rows to look for 'Statut': the type of channel
        """
        for row in rows:
            if 'Statut' in row.text:
                try:
                    return self.remove_parenthesis_substr(row.find('div').text.strip()).split(' ')
                except AttributeError:
                    return None

    def parse_type_list(self, type_list):
        """
        Checks for type of news channel and for its privatization status
        """
        if type_list is None:
            return None, None
        theme = [el for el in type_list if el.title() in ('Généraliste', 'Thématique')]
        ownership = [el for el in type_list if el.lower() in ('publique', 'privée')]
        theme = None if len(theme)==0 else theme[0]
        ownership = None if len(ownership)==0 else ownership[0]
        return theme, ownership

    def get_audiences_table(self, html):
        """
        Get audiences table (not complete)
        """
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.findAll('table')
        for table in tables:
            contains_caption = table.find('caption')
            if contains_caption is not None:
                if 'audiences' in contains_caption.text.lower():
                    print(contains_caption.text)
                    return table
        #mw-content-text > div.mw-parser-output > table:nth-child(98) > tbody

    def post_process_results(self, results):
        """
        Maps types to english words
        """
        results.type = results.type.map({'Généraliste': 'general', 'Thématique': 'news'})
        results.privatization_status = results.privatization_status.map({'publique': 'public', 'privée': 'private'})
        results.index.name = 'channel'
        return results
    
    def search_channels(self):
        results = {}
        for idx, channel in enumerate(self.channel_list):
            channel_name = channel.replace(' ', '_').lower()
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
            try:
                results[channel_name] = {}
                html = self.get_html(page)
                rows = self.get_html_summary_table(html)
                type_list = self.get_channel_type(rows)
                theme, ownership = self.parse_type_list(type_list)
                results[channel_name]['type'] = theme
                results[channel_name]['privatization_status'] = ownership
                results[channel_name]['group'] = self.get_group(rows)
            except TypeError:
                continue
        return self.post_process_results(pd.DataFrame.from_dict(results, orient='index'))

    
    def generate_page_content(self, n_fetch=None):
        results = {}
        n_fetch = len(self.channel_list) if n_fetch is None else n_fetch
        for idx, channel in enumerate(self.channel_list[:n_fetch]):
            channel_name = channel.replace(' ', '_').lower()
            try:
                searches = wikipedia.search(channel)
                if len(searches) == 0:
                    print(f'Nothing found for {channel}')
                    continue
                page = wikipedia.page(searches[0], auto_suggest=False)
            except wikipedia.exceptions.DisambiguationError:
                try:
                    searches = wikipedia.search(channel)
                    page = wikipedia.page(searches[0] + ' (chaîne de télévision)', auto_suggest=False)
                except wikipedia.exceptions.PageError:
                    print(f'Nothing found for {channel}')
                    page = None
            if page is not None:
                yield channel_name, page.content.replace("\n", "")
            else:
                yield channel_name, None


if __name__ == '__main__':
    manager = WikiChannelDataManager(os.path.join(WIKI_FILE_PATH, '../../../data/channels.xlsx'))
    results = manager.search_channels()
    if not os.path.isdir(os.path.join(WIKI_FILE_PATH, '../../../data/channels/')):
        os.mkdir(os.path.join(WIKI_FILE_PATH, '../../../data/channels/'))
    results.to_csv(os.path.join(WIKI_FILE_PATH, '../../../data/channels/scraped_channels.csv'))