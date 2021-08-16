import re
from datetime import datetime

import scrapy
from scrapy.utils.response import open_in_browser

# from institutes import Institutes


# function to convert string to camelCase
def camelcase(string):
  string = re.sub(r"(_|-)+", " ", string).title().replace(" ", "")
  return string[0].lower() + string[1:]


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def parse_sched_class_title(title):
    parts = [part.strip() for part in title.split(' - ')]

    if len(parts) < 4:
        print('Welp! this title is borked', parts)

    section = parts[-1]
    course  = parts[-2].split(' ')[-1]
    subject = ' '.join(parts[-2].split(' ')[:-1])
    crn     = parts[-3]
    title   = ' - '.join(parts[:-3])

    return {
        'courseReferenceNumber': crn,
        'subject': subject.replace(' ', ''),
        'course': course,
        'section': section,
        'title': title,
    }


import re
from collections import defaultdict
from datetime import datetime
# from hashlib import sha224

from bs4 import BeautifulSoup


class BaseHooks:
    DATE_FORMAT = ['%b %d, %Y', '%d-%b-%Y'] # '%d-%b-%Y'
    TIME_FORMAT = '%I:%M %p' # '%H:%M'
    UNITS_STR = [' Credits', ' Continuing Education Units']

    PUBLIC_SCHED_TH_CLASSNAME = 'ddtitle'
    PUBLIC_SCHED_TD_CLASSNAME = 'dddefault'

    @staticmethod
    def transform_class(class_data):
        return class_data

    @classmethod
    def parse_date(cls, date_str):
        for pattern in cls.DATE_FORMAT:
            try:
                return datetime.strftime(datetime.strptime(date_str, pattern), '%m/%d/%Y')
            except:
                pass

        raise RuntimeError(f'None of the date patterns match for date_str="{date_str}"')

    @staticmethod
    def clean_units_str(units_str):
        if 'TO' in units_str:
            splitted = units_str.split('TO')
            return splitted[-1].strip()

        elif 'OR' in units_str:
            splitted = units_str.split('OR')
            return splitted[-1].strip()

        else:
            return units_str

    @staticmethod
    def clean_instructor_name(name):
        # Replace ', ' with '', '(P)' with '', '   ' (n spaces) with ' ' (one space)
        return re.sub(r'\s+', ' ', re.sub(r'(?:, )|(?:\(\w?\))', '', name)).strip()


class PublicScheduleMiner:
    PREFIX = 'sched_'
    SOUP_PARSER = 'lxml'

    def __init__(self, logger):
        self.logger = logger
        self.hooks = BaseHooks()

    def mine(self, html, institute, term):
        soup = BeautifulSoup(html, self.SOUP_PARSER)
        rows = soup.select('.pagebodydiv > table.datadisplaytable > tr')

        # if self.SOUP_PARSER == 'html5lib':
        #     rows = soup.select('.pagebodydiv > table.datadisplaytable > tbody > tr')
        # else:
        #     rows = soup.select('.pagebodydiv > table.datadisplaytable > tr')

        last_class = None

        for table_row in rows:
            ths = table_row.find_all('th', {'class': self.hooks.PUBLIC_SCHED_TH_CLASSNAME}, recursive=False)
            tds = table_row.find_all('td', {'class': self.hooks.PUBLIC_SCHED_TD_CLASSNAME}, recursive=False)

            if len(ths) > 0 and len(tds) == 0:
                # print('Header', headers[0])
                text = ths[0].get_text().strip()
                parts = [part.strip() for part in text.split(' - ')]

                if len(parts) < 4:
                    self.logger.error(
                        'Unable to parse public schedule course title due to unknown format | '
                        f'institute={institute}, term={term}, parts={parts}'
                    )
                    continue

                section = parts[-1]
                course  = parts[-2].split(' ')[-1]
                dept    = ' '.join(parts[-2].split(' ')[:-1])
                crn     = parts[-3]
                title   = ' - '.join(parts[:-3])

                data = {
                    'CRN': crn,
                    'raw_course': f'{dept} {course}{section}',
                    'dept': dept.replace(' ', ''),
                    'course': course,
                    'section': section,
                    'title': title,
                    'times': [],
                }

                last_class = data
                # print(section, dept, course, crn, title)

            elif len(ths) == 0 and len(tds) == 1:
                # print('Data', tds[0])
                data = last_class
                data_col = tds[0]

                if data_col.get('bgcolor') == 'yellow':
                    continue

                if not data:
                    print('Skipping cause who knows what this is?', data_col)
                    continue

                more_details = defaultdict(str)

                def loop_on_children(children):
                    prev_label = None

                    for el in children:
                        if isinstance(el, str):
                            if el == '\n':
                                pass
                                # prev_label = None
                            elif prev_label:
                                more_details[prev_label] += el.strip()
                            else:
                                text = el.strip()
                                for units_str in self.hooks.UNITS_STR:
                                    if units_str in text:
                                        units = text.replace(units_str, '').strip()
                                        data['units'] = self.hooks.clean_units_str(units)
                                    else:
                                        pass
                                        # print('Unhandled', el)
                        else:
                            if el.name == 'br':
                                prev_label = None

                            elif el.name == 'span' and el.get('class') and 'fieldlabeltext' in el['class']:
                                label = el.get_text().strip().replace(':', '')
                                prev_label = label

                            elif el.name == 'table' and el.get('class') and 'datadisplaytable' in el['class']:
                                times = self.parse_inner_table(institute, term, el)

                                for time in times:
                                    if 'start' in time and 'end' in time:
                                        data['start'] = self.hooks.parse_date(time['start'])
                                        data['end'] = self.hooks.parse_date(time['end'])
                                        break

                                data['times'] = times

                            elif el.name == 'a':
                                pass

                            elif el.name == 'font' or el.name == 'b':
                                loop_on_children(el.contents)

                            else:
                                # print('Unhandled', el)
                                pass

                loop_on_children(data_col.contents)

                if 'start' not in data:
                    data['start'] = 'TBA'
                if 'end' not in data:
                    data['end'] = 'TBA'

                if not data.get('units') and more_details.get('Credits'):
                    data['units'] = self.hooks.clean_units_str(more_details['Credits'])

                if not data.get('units'):
                    html = str(data_col).split('\n')
                    for line in html:
                        for units_str in self.hooks.UNITS_STR:
                            matches = re.match(fr'^\s*(.*)\s*{units_str}\s*', line)
                            if matches and matches.groups():
                                units = matches.groups()[0].strip()
                                data['units'] = self.hooks.clean_units_str(units)

                if not data.get('units'):
                    self.logger.error(
                        'Skipping class because units were not found | '
                        f'institute={institute}, term={term}, data={data}, more_details={more_details}',
                    )
                    continue

                yield self.hooks.transform_class(data)

                last_class = None

            elif len(ths) == 0 and len(tds) == 0:
                pass

            else:
                print('Unhandled row!', ths, tds)


    def parse_inner_table(self, institute, term, table):
        rows = table.find_all('tr')

        table_headers = []
        times = []

        for table_row in rows:
            ths = table_row.find_all('th', recursive=False)
            tds = table_row.find_all('td', recursive=False)

            headers = [th.get_text().strip() for th in ths]
            data_cols = [td.get_text().strip() for td in tds]

            if len(headers) > 0 and len(data_cols) == 0:
                table_headers = headers

            elif len(headers) == 0 and len(data_cols) > 0:
                data = dict(zip(table_headers, data_cols))
                dates = data.get('Date Range')

                if len(data_cols) != len(table_headers):
                    {'headers': table_headers, 'data': data_cols}
                    self.logger.warning(
                        'Skipping times row because the # of data columns do not match the # of table header columns | '
                        f'institute={institute}, term={term}, headers={table_headers}, data={data_cols}',
                    )
                    continue

                instr_td = tds[table_headers.index('Instructors')]
                instructors = []
                last_name = ''

                def add_partial_last():
                    if last_name:
                        full_name = self.hooks.clean_instructor_name(last_name)
                        # pretty_id = full_name.lower().replace(' ', '-')
                        instructors.append({
                            # 'id': sha224(pretty_id.encode()).hexdigest(),
                            # 'pretty_id': pretty_id,
                            'full_name': full_name
                        })

                for node in instr_td.contents:
                    if isinstance(node, str):
                        if node.strip().startswith(','):
                            add_partial_last()
                            last_name = node
                        else:
                            last_name += node
                    else:
                        if node.name == 'a':
                            full_name = self.hooks.clean_instructor_name(last_name)
                            email = node.get('href').replace('mailto:', '').strip()

                            instructors.append({
                                # 'id': sha224(email.encode()).hexdigest(),
                                # 'pretty_id': full_name.lower().replace(' ', '-'),
                                'full_name': full_name,
                                'display_name': node.get('target').strip(),
                                'email': email
                            })
                            last_name = ''

                        elif node.name == 'abbr':
                            last_name  += node.get_text()
                            pass

                        else:
                            print('idk what this is', node)

                add_partial_last()

                if not dates or dates == 'TBA':
                    start = 'TBA'
                    end = 'TBA'
                elif ' - ' in dates:
                    start = dates.split(' - ')[0].strip()
                    end = dates.split(' - ')[1].strip()
                else:
                    start = 'TBA'
                    end = 'TBA'
                    print('This is just stupiiid')

                # campus = ' '.join(data.get('Where').split(' ')[:-1])

                class_time = {
                    'type': data.get('Type'),
                    'days': data.get('Days'),
                    'time': data.get('Time'),
                    'instructor': instructors,
                    'location': data.get('Where') or 'TBA',
                    # 'instructor': data.get('Instructors'),
                    # 'room': data.get('Where').split(' ')[-1],
                    # 'campus': campus,

                    'start': start,
                    'end': end,
                }

                if class_time['days'] and class_time['days'] != 'TBA':
                    class_time['days'] = class_time['days'].replace('R', 'Th')

                times.append(class_time)

            else:
                print('Unhandled stuff')

        return times


class Banner8Spider(scrapy.Spider):
    name = 'banner8'

    TERM_LIMIT = 1

    # def __init__(self, institutes: Institutes, *args, **kwargs):
    def __init__(self, institutes, *args, **kwargs):
        super(Banner8Spider, self).__init__(*args, **kwargs)

        self.institutes = institutes
        self.miner = PublicScheduleMiner(self.logger)

    def start_requests(self):
        entrypoints = {'bwckschd.p_disp_dyn_sched': self.parse_schedule_sel_term}

        for institute in self.institutes:
            for path, handler in entrypoints.items():
                yield scrapy.Request(
                    # institute.source.url + path,
                    institute['source']['url'] + path,
                    callback=handler,
                    # meta={'resource_type': 'schedule_sel_term', 'institute': institute.id},
                    meta={'resource_type': 'schedule_sel_term', 'institute': institute['id']},
                )

    # 12m 43s for (fhda, wvm, merced) 1245 class details (autothrottle off, 1.5s delay, 4 conc_req/domain)
    #  1m 36s for (fhda, wvm, merced) 5034 class details (autothrottle  on,   0s delay, 5 target conc)
    # 14m 57s for (fhda, wvm, merced) 5034 class details (autothrottle  on, 0.4s delay, 2 target conc (useless))

    # def start_requests(self):
    #     for banner in self.banners:
    #         yield scrapy.Request(
    #             banner['url'],
    #             meta={'banner': banner},
    #             callback=self.parse
    #         )

    # def parse(self, response):
    #     links = response.css('a::attr(href)').getall()

    #     for link in links:
    #         path = link.split('/')[-1]

    #         if path in self.KNOWN_LINKS.keys():
    #             yield response.follow(
    #                 link,
    #                 callback=getattr(self, f'parse_{self.KNOWN_LINKS[path]}'),
    #                 meta={'banner': response.meta['banner']},
    #             )

    def parse_schedule_sel_term(self, response):
        for i, option in enumerate(response.css('[name="p_term"] option')):
            code = option.attrib['value'].strip()
            description = option.css('::text').get().strip()

            if code == '' or code == '%':
                continue

            view_only = 'view only' in description.lower()
            title = re.sub(r'\(View only\)',  '', description, flags=re.IGNORECASE).strip()

            # TODO: choose the smaller of the two years instead of the one that comes first
            match_year = re.search(r'([0-9]{4})(?: ?- ?([0-9]{4}))?', description)
            year = match_year and match_year.group(1) and int(match_year.group(1))

            # strip year from title
            cl2 = title.replace(match_year and match_year.group(0) or '', '')
            # replace non-alphanumeric/whitespace characters with spaces
            cl3 = re.sub(r'[^\w\s]+', ' ', cl2).strip()
            # replace continuous segments of whitespace with -
            term_name = re.sub(r'\s+', '-', cl3).lower()

            # print(year, cleaned)
            # print(title)

            current_year = datetime.now().year

            # if i > self.TERM_LIMIT:
            if not (year and current_year - year <= 0):
                return

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

            yield scrapy.FormRequest.from_response(
                response,
                formdata={'p_term': code},
                callback=self.parse_schedule_search,
                meta={'resource_type': 'schedule_search', 'institute': response.meta['institute'], 'term': code},
            )

    def parse_schedule_search(self, response):
        subjects = []

        for option in response.css('[name="sel_subj"] option'):
            code = option.attrib['value'].strip()
            description = option.css('::text').get().strip()

            if code == '' or code == '%':
                continue

            yield {
                'type': 'subject',
                'institute': response.meta['institute'],
                'term': response.meta['term'],
                'code': code,
                'description': description,
            }

            subjects.append(code)

        for chunk_subjects in batch(subjects, 30):
            yield scrapy.FormRequest.from_response(
                response,
                formdata=[('sel_subj', 'dummy'), *(('sel_subj', code) for code in chunk_subjects)],
                callback=self.parse_schedule_results,
                meta={'resource_type': 'schedule_results', 'institute': response.meta['institute'], 'term': response.meta['term']},
            )

    def parse_schedule_results(self, response):
        for item in self.miner.mine(response.text, institute=response.meta['institute'], term=response.meta['term']):
            yield {
                'type': 'class',
                'institute': response.meta['institute'],
                'term': response.meta['term'],
                **item,
            }

        # titles = response.css('.ddtitle a')

        # # if len(titles) == 0:
        # #     open_in_browser(response)

        # for i, a in enumerate(titles):
        #     yield {
        #         'type': 'class',
        #         'institute': response.meta['institute'],
        #         'term': response.meta['term'],
        #         **(parse_sched_class_title(a.css('::text').get())),
        #     }

            # yield response.follow(
            #     a.attrib['href'],
            #     priority=-1,
            #     callback=self.parse_class_detail,
            #     meta={'resource_type': 'class_detail', 'institute': response.meta['institute'], 'term': response.meta['term']},
            # )

    def parse_class_detail(self, response):
        seatData = {}

        title = response.css('.pagebodydiv > .datadisplaytable > tr:first-of-type > th:first-child::text').get()
        seatTableRows = response.css('.datadisplaytable .datadisplaytable tr')
        headers = []

        for row in seatTableRows:
            dead = row.css('.dddead').get()
            heads = row.css('.ddheader span::text').getall()
            label = row.css('.ddlabel span::text').get()
            default = row.css('.dddefault::text').getall()

            if dead and len(heads):
                headers = [camelcase(head.strip()) for head in heads]
            elif label and len(default) > 0:
                seatData[camelcase(label.strip())] = dict(zip(headers, [int(text.strip()) for text in default]))

        yield {
            'type': 'class_seats',
            'institute': response.meta['institute'],
            'term': response.meta['term'],
            'seats': seatData,
            **(parse_sched_class_title(title)),
        }
