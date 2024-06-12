import aiohttp
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm_asyncio
from bs4 import BeautifulSoup as bs
import os

class rbc_parser:
    def __init__(self):
        pass

    def _get_url(self, param_dict: dict) -> str:
        """
        Возвращает URL для запроса json таблицы со статьями
        """
        url = 'https://www.rbc.ru/search/ajax/?' +\
        'project={0}&'.format(param_dict['project']) +\
        'dateFrom={0}&'.format(param_dict['dateFrom']) +\
        'dateTo={0}&'.format(param_dict['dateTo']) +\
        'page={0}&'.format(param_dict['page']) +\
        'query={0}&'.format(param_dict['query']) +\
        'material={0}'.format(param_dict['material'])
        
        return url

    async def _get_search_table(self, session, param_dict: dict) -> pd.DataFrame:
        """
        Возвращает pd.DataFrame со списком статей
        """
        url = self._get_url(param_dict)
        async with session.get(url) as response:
            r = await response.json()
            search_table = pd.DataFrame(r['items'])
            if 'publish_date_t' in search_table.columns:
                search_table.sort_values('publish_date_t', ignore_index=True)
            return search_table

    async def _get_article_body(self, session, url: str) -> str:
        """
        Возвращает текст статьи по URL
        """
        async with session.get(url) as response:
            r = await response.text()
            soup = bs(r, features="lxml")
            p_text = soup.find_all('p')
            if p_text:
                text = ' '.join(map(lambda x: x.text.replace('<br />', '\n').strip(), p_text))
            else:
                text = None
            return text

    async def _iterable_load_by_page(self, session, param_dict, pbar):
        param_copy = param_dict.copy()
        results = []
        result = await self._get_search_table(session, param_copy)
        if not result.empty:
            urls = result['fronturl'].tolist()
            bodies = await asyncio.gather(*[self._get_article_body(session, url) for url in urls])
            result['body'] = bodies
        results.append(result)
        pbar.update(1)
        
        while not result.empty:
            param_copy['page'] = str(int(param_copy['page']) + 1)
            result = await self._get_search_table(session, param_copy)
            if not result.empty:
                urls = result['fronturl'].tolist()
                bodies = await asyncio.gather(*[self._get_article_body(session, url) for url in urls])
                result['body'] = bodies
            results.append(result)
            pbar.update(1)
        
        results = pd.concat(results, axis=0, ignore_index=True)
        return results

    async def get_articles(self, param_dict, time_step=1, save_every=5, save_jsonl=True):
        """
        Функция для скачивания статей интервалами через каждые time_step дней
        Делает сохранение таблицы через каждые save_every * time_step дней
        """
        param_copy = param_dict.copy()
        time_step = timedelta(days=time_step)
        dateFrom = datetime.strptime(param_copy['dateFrom'], '%d.%m.%Y')
        dateTo = datetime.strptime(param_copy['dateTo'], '%d.%m.%Y')
        if dateFrom > dateTo:
            raise ValueError('dateFrom should be less than dateTo')

        out = pd.DataFrame()
        save_counter = 0
        total_steps = (dateTo - dateFrom).days // (time_step.days + 1) + 1

        async with aiohttp.ClientSession() as session:
            with tqdm_asyncio(total=total_steps, desc="Loading articles") as pbar:
                while dateFrom <= dateTo:
                    param_copy['dateTo'] = (dateFrom + time_step).strftime("%d.%m.%Y")
                    if dateFrom + time_step > dateTo:
                        param_copy['dateTo'] = dateTo.strftime("%d.%m.%Y")
                    print('Parsing articles from ' + param_copy['dateFrom'] + ' to ' + param_copy['dateTo'])
                    out = pd.concat([out, await self._iterable_load_by_page(session, param_copy, pbar)], axis=0, ignore_index=True)
                    dateFrom += time_step + timedelta(days=1)
                    param_copy['dateFrom'] = dateFrom.strftime("%d.%m.%Y")
                    save_counter += 1
                    if save_counter == save_every:
                        try:
                            checkpoint_file = "./tmp/checkpoint_table.jsonl"
                            if os.path.exists(checkpoint_file):
                                existing_data = pd.read_json(checkpoint_file, orient="records", lines=True)
                                out = pd.concat([existing_data, out], ignore_index=True)
                            out[["id", "title", "publish_date", "body"]].to_json(checkpoint_file, orient="records", lines=True, force_ascii=False)
                            print('Checkpoint saved!')
                        except KeyError as e:
                            print(f"KeyError: {e}. Available columns: {out.columns}")
                        save_counter = 0

        if save_jsonl:
            try:
                final_file = "rbc_{}_{}.jsonl".format(param_dict['dateFrom'], param_dict['dateTo'])
                if os.path.exists(final_file):
                    existing_data = pd.read_json(final_file, orient="records", lines=True)
                    out = pd.concat([existing_data, out], ignore_index=True)
                out[["id", "title", "publish_date", "body"]].to_json(final_file, orient="records", lines=True, force_ascii=False)
            except KeyError as e:
                print(f"KeyError: {e}. Available columns: {out.columns}")
        print('Finish')

        return out

if __name__ == "__main__":
    param_dict = {
        'project': 'rbcnews',
        'dateFrom': '01.01.2010',
        'dateTo': '02.06.2024',
        'page': '1',
        'query': 'РБК',
        'material': 'article'
    }
    
    parser = rbc_parser()
    asyncio.run(parser.get_articles(param_dict))
