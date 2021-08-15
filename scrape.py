import gzip

from scrapy.exporters import JsonLinesItemExporter
from scrapy.crawler import CrawlerProcess

from banner9 import Banner9Spider


# https://github.com/scrapy/scrapy/issues/2174#issuecomment-283259507
class JsonLinesGzipItemExporter(JsonLinesItemExporter):
    def __init__(self, file, **kwargs):
        gzfile = gzip.GzipFile(fileobj=file)
        super(JsonLinesGzipItemExporter, self).__init__(gzfile, **kwargs)

    def finish_exporting(self):
        self.file.close()


def scrape():
    process = CrawlerProcess(settings={
        'LOG_LEVEL': 'INFO',
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36',
        'FEEDS': {
            'out/scrape-banner9.jl.gz': {'format': 'jl.gz'},
        },
        'FEED_EXPORTERS': {
            'jl.gz': '__main__.JsonLinesGzipItemExporter',
        },
    })

    process.crawl(Banner9Spider)
    process.start()


if __name__ == '__main__':
    scrape()
