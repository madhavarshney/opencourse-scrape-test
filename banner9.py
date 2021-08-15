import re
import json
from datetime import datetime
from urllib.parse import urlparse

import scrapy

# from institutes import institutes
# from institutes.models import BannerEightDataSource, BannerNineDataSource


class Banner9Spider(scrapy.Spider):
    name = 'banner9'

    def start_requests(self):
        with open('institutes.json') as f:
            institutes = json.load(f)

        for institute in institutes:
            # if isinstance(institute.source, BannerNineDataSource):
            if institute['source']['type'] == 'banner9':
                # if institute.source.config.get('mepCode', None):
                if institute['source']['config'].get('mepCode', None):
                    continue

                # domain = urlparse(institute.source.url).netloc
                # ssb_url = institute.source.url
                domain = urlparse(institute['source']['url']).netloc
                ssb_url = institute['source']['url']

                yield scrapy.Request(
                    f'{ssb_url}ssb/classSearch/getTerms?offset=1&max=20',
                    callback=self.parse_terms,
                    meta={'ssb_url': ssb_url, 'institute': institute['id'] or domain},
                    # meta={'ssb_url': ssb_url, 'institute': institute.id or domain},
                    # meta={'resource_type': 'schedule_sel_term', 'institute': institute.id},
                )

    def parse_terms(self, response):
        data = response.json()

        for term in data:
            code = term['code']
            description = term['description']

            view_only = 'view only' in description.lower()
            title = re.sub(r'\(View only\)',  '', description, flags=re.IGNORECASE).strip()

            # TODO: choose the smaller of the two years instead of the one that comes first
            match_year = re.search(r'([0-9]{4})(?: ?[-/] ?([0-9]{4}))?', description)
            year = match_year and match_year.group(1) and int(match_year.group(1))

            # strip year from title
            cl2 = title.replace(match_year and match_year.group(0) or '', '')
            # replace non-alphanumeric/whitespace characters with spaces
            cl3 = re.sub(r'[^\w\s]+', ' ', cl2).strip()
            # replace continuous segments of whitespace with -
            term_name = re.sub(r'\s+', '-', cl3).lower()

            yield {
                'type': 'term',
                'institute': response.meta['institute'],
                'code': code,
                'title': title,
                'description': description,
                'view_only': view_only,
                'year': year,
                'term_name': term_name,
            }

            current_year = datetime.now().year

            # if i > self.TERM_LIMIT:
            if view_only or not (year and current_year - year <= 0):
                continue

            yield scrapy.Request(
                f"{response.meta['ssb_url']}ssb/classSearch/get_subject?term={code}&offset=1&max=500",
                callback=self.parse_term_class_subjects,
                meta={
                    'ssb_url': response.meta['ssb_url'],
                    'institute': response.meta['institute'],
                    'term': code
                },
            )

            # TODO: wrong referrers
            yield scrapy.Request(
                f'{response.meta["ssb_url"]}ssb/term/search?term={code}',
                callback=self.start_fetching_classes,
                meta={
                    'ssb_url': response.meta['ssb_url'],
                    'institute': response.meta['institute'],
                    'term': code,
                    'cookiejar': f"{response.meta['institute']}-{code}"
                },
            )

    def parse_term_class_subjects(self, response):
        data = response.json()

        for subject in data:
            yield {
                'type': 'subject',
                'institute': response.meta['institute'],
                'term': response.meta['term'],
                'code': subject['code'],
                'description': subject['description'],
            }

    def start_fetching_classes(self, response):
        yield self.get_search_results(response.meta)

    def get_search_results(self, meta, offset = 0):
        return scrapy.Request(
            f'{meta["ssb_url"]}ssb/searchResults/searchResults?txt_term={meta["term"]}&pageOffset={offset}&pageMaxSize=500',
            callback=self.parse_classes,
            meta={
                'ssb_url': meta['ssb_url'],
                'institute': meta['institute'],
                'term': meta['term'],
                'cookiejar': meta['cookiejar'],
                'class_length': offset,
            },
        )

    def parse_classes(self, response):
        data = response.json()

        success = data['success']
        total_count = data['totalCount']
        items = data['data']

        if success and items:
            for class_ in items:
                yield {
                    'type': 'class',
                    'institute': response.meta['institute'],
                    'term': response.meta['term'],
                    **class_,
                }

            if total_count > response.meta['class_length'] + len(items):
                yield self.get_search_results(response.meta, response.meta['class_length'] + len(items))
