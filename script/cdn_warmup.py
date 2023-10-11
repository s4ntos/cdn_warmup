#!/usr/bin/env python3
import os
import datetime
import argparse
import functools
import asyncio
import aiohttp
# import requests
import pandas as pd
# from lxml import etree, html
from tabulate import tabulate

parser = argparse.ArgumentParser(description='Asynchronous CDN Cache Warmer')
parser.add_argument('-f', '--file', action="store", dest='file', default=None)
parser.add_argument('-c', '--concurrency', action="store", dest='concurrency', default=1)
parser.add_argument('-t', '--timeout', action="store", dest='timeout', default=2)
parser.add_argument('-o', '--output',  action="store_true", default=None)
parser.add_argument('-q', '--quiet', action="store_true", help="Only print 40x, 50x or 200 with noindex")
args = parser.parse_args()
concurrency = args.concurrency
timeout = args.timeout
file = args.file
quiet = args.quiet
output = args.output

tasks = []
tab_headers = ['Response code', 'Time (ms)', 'Age', 'Cache Hit']
results = pd.DataFrame(columns=['url','http_code', 'time', 'age', 'x-cache'])
dot = 0
dot_total = 0
failed_links = 0
success_links = 0
domain = ''
http_headers = {'User-Agent': 'CDNWarmup/1.0'}


def get_links(file):
    links = pd.read_csv(file)
    return links['IMAGE_LINK']


async def bound_warms(sem, url):
    async with sem:
        await warm_it(url)


async def warm_it(url):

    connection_started_time = None
    connection_made_time = None

    class TimedResponseHandler(aiohttp.client_proto.ResponseHandler):
        def connection_made(self, transport):
            nonlocal connection_made_time
            connection_made_time = datetime.datetime.now()
            return super().connection_made(transport)

    class TimedTCPConnector(aiohttp.TCPConnector):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._factory = functools.partial(TimedResponseHandler, loop=loop)

        async def _create_connection(self, req, traces, timeout):
            nonlocal connection_started_time
            connection_started_time = datetime.datetime.now()
            return await super()._create_connection(req, traces, timeout)
    session_timeout =   aiohttp.ClientTimeout(total=None,sock_connect=timeout,sock_read=timeout)
    async with aiohttp.ClientSession(connector=TimedTCPConnector(loop=loop), headers=http_headers, timeout=session_timeout) as session:
        try: 
            async with session.get(url,timeout=timeout) as response:
                global dot, dot_total, results
                if output is True:
                    print(f'Preparing to retrieve url : {url}')
                time_delta = connection_made_time - connection_started_time
                cache_hit = 'Not found'
                age = 0

                if response.status != 200:
                    response_output = response.status
                else:
                    response_output = response.status
                    age = response.headers['age']
                    if 'x-cache' in response.headers :
                        cache_hit = response.headers['x-cache']
                        
                res = { 'url' : url,
                        'http_code': response_output,
                        'time': time_delta,
                        'age': age,
                        'x-cache' : cache_hit
                    }
                results.loc[len(results)] = res
                del response
        except Exception as e:
            res = { 'url' : url,
                    'http_code': 000,
                    'time': None ,
                    'age' : None ,
                    'x-cache' : e
                  }
            results.loc[len(results)] = res


def write_list_to_csv(file_processed, headers, results):
    url_file = os.path.basename(a.path)
    filename = url_file.split('.')

    results.to_csv(f'{filename[0]}.csv', encoding='utf-8', index=False)


def main():

    global success_links, failed_links, time_array, results, domain, tab_headers

    if concurrency is None:
        print("The concurrency limit isn't specified. Setting limit to 150")
    else:
        print("Setting concurrency limit to %s Quiet: %s Output: %s" % (concurrency, quiet, output))
        print("\n")

    mage_links = get_links(file)

    if len(mage_links) > 0:
        for i in mage_links:
            task = asyncio.ensure_future(bound_warms(sem, i))
            tasks.append(task)
        
        loop.run_until_complete(asyncio.wait(tasks))
        
        print(f'Took on average : {results["time"].mean()}\n')
        print(tabulate(results.groupby(['http_code']).size().to_frame(), ['HTTP Code','Count'], tablefmt="simple") + '\n')
        print(tabulate(results.groupby(['x-cache']).size().to_frame(), ['Cache Hit', 'Count'], tablefmt="simple") + '\n')
        
        if output is True:
            write_list_to_csv(file, tab_headers, results)

        del mage_links
        results = results.iloc[0:0]


if __name__ == "__main__":

    sem = asyncio.Semaphore(int(concurrency))
    loop = asyncio.get_event_loop()

    # execute only if run as a script
    main()
    
    loop.close()

'''
            if results is not None:
'''