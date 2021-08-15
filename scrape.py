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
        'USER_AGENT'
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
