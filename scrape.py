import sys
import gzip
import json

from scrapy.exporters import JsonLinesItemExporter
from scrapy.crawler import Crawler, CrawlerProcess

from banner8 import Banner8Spider
from banner9 import Banner9Spider

SCRAPY_SETTINGS = {
    'LOG_LEVEL': 'INFO',
    'CONCURRENT_REQUESTS': 150,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
    'DOWNLOAD_DELAY': 0.25,
    'DEPTH_PRIORITY': 1,
    'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
    'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
    'FEED_EXPORTERS': {
        'jl.gz': '__main__.JsonLinesGzipItemExporter',
    },
}


# https://github.com/scrapy/scrapy/issues/2174#issuecomment-283259507
class JsonLinesGzipItemExporter(JsonLinesItemExporter):
    def __init__(self, file, **kwargs):
        gzfile = gzip.GzipFile(fileobj=file, mode='wb')
        super(JsonLinesGzipItemExporter, self).__init__(gzfile, **kwargs)

    def finish_exporting(self):
        self.file.close()


def scrape_b8():
    with open('institutes.json') as f:
        institutes = json.load(f)

    process = CrawlerProcess(settings={
        **SCRAPY_SETTINGS,
        # 'JOBDIR': 'out/crawls/banner8/',
        # 'CONCURRENT_REQUESTS': 40,
        'FEEDS': {
            'out/scrape-banner8.jl.gz': {'format': 'jl.gz'},
        },
    })

    process.crawl(Banner8Spider, [i for i in institutes if i['source']['type'] == 'banner8'])
    process.start()


def scrape_b9():
    process = CrawlerProcess(settings={
        **SCRAPY_SETTINGS,
        # 'JOBDIR': 'out/crawls/banner9/',
        # 'CONCURRENT_REQUESTS': 100,
        'FEEDS': {
            'out/scrape-banner9.jl.gz': {'format': 'jl.gz'},
        },
    })

    process.crawl(Banner9Spider)
    process.start()


def scrape_both():
    with open('institutes.json') as f:
        institutes = json.load(f)

    process = CrawlerProcess(settings=SCRAPY_SETTINGS)
    crawl_b8 = Crawler(Banner8Spider, {
        **SCRAPY_SETTINGS,
        'FEEDS': {
            'out/scrape-banner-v8.jl.gz': {'format': 'jl.gz'},
        },
    })
    crawl_b9 = Crawler(Banner9Spider, {
        **SCRAPY_SETTINGS,
        'FEEDS': {
            'out/scrape-banner-v9.jl.gz': {'format': 'jl.gz'},
        },
    })

    process.crawl(crawl_b8, [i for i in institutes if i['source']['type'] == 'banner8'])
    process.crawl(crawl_b9)
    process.start()


if __name__ == '__main__':
    try:
        spider = sys.argv[1]
    except IndexError:
        print('No spider specified. Choose either "banner8" or "banner9".')
        sys.exit(1)

    if spider == 'banner8':
        scrape_b8()
    elif spider == 'banner9':
        scrape_b9()
    elif spider == 'both':
        scrape_both()
    else:
        print('Unknown spider specified. Choose either "banner8" or "banner9".')
