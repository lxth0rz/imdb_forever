import re
import os
import apify
import logging
from scrapy import Spider
from urllib.parse import urljoin
from apify_client import ApifyClient
from scrapy.http.request import Request


class ImdbMoviesByCompanyNameScraper(Spider):

    name = 'imdb_movies_by_companies'

    headers = {'Host': 'www.imdb.com',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:101.0) Gecko/20100101 Firefox/101.0',
               'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
               'Accept-Language': 'en-GB,en;q=0.5',
               'Accept-Encoding': 'gzip, deflate',
               'Connection': 'keep-alive',
               'Upgrade-Insecure-Requests': '1', }

    company_id = ''      # required input
    testing = False      # optional for testing - scraping only 50 movies

    imdb_by_company_base_url = 'https://www.imdb.com/search/title/?companies={0}'

    regex_dict = {'movie_id': 'tt\d+'}

    xpath_dict = {'movie_box': '//*[@class="lister-item mode-advanced"]',
                  'title': './/a[contains(@href, "/title/")]/text()',
                  'url': './/a[contains(@href, "/title/")]/@href',
                  'year': './/span[@class="lister-item-year text-muted unbold"]/text()',
                  'poster': './/a/img/@loadlate',
                  'certificate': './/span[@class="certificate"]/text()',
                  'runtime': './/span[@class="runtime"]/text()',
                  'genre': './/span[@class="genre"]/text()',
                  'rating': './/div[@class="ratings-bar"]/.//strong/text()',
                  'plot': './/p[@class="text-muted"]/text()',
                  'stars': './/*[contains(text(), "Stars")]/.//a/text()',
                  'stars_failed': './/p[@class=""]/.//text()',
                  'votes': './/p[@class="sort-num_votes-visible"]/span/text()',
                  'next_overview': './/a[contains(text(), "Next")]/@href',
                  'companies_rows': './/table[@class="findList"]/.//tr',
                  'company_link': './/a/@href',
                  'company_row_full_text': './/text()'}

    errors = {'CompanyNotFound': 'Cannot find company, type and country!'}

    logger = None

    def start_requests(self):

        self.logger = logging.getLogger()

        # Initialize the main ApifyClient instance
        client = ApifyClient(os.environ['APIFY_TOKEN'], api_url=os.environ['APIFY_API_BASE_URL'])

        # Get the resource subclient for working with the default key-value store of the actor
        default_kv_store_client = client.key_value_store(os.environ['APIFY_DEFAULT_KEY_VALUE_STORE_ID'])

        # Get the value of the actor input and print it
        self.logger.info('Loading input...')
        actor_input = default_kv_store_client.get_record(os.environ['APIFY_INPUT_KEY'])['value']
        self.logger.info(actor_input)

        self.company_id = actor_input["CompanyId"]
        self.testing = actor_input["Testing"]

        if self.company_id != '':
            start_url = self.imdb_by_company_base_url.format(self.company_id)
            yield Request(url=start_url,
                          headers=self.headers,
                          callback=self.parse_overview_page)
        else:
            self.logger.error('Company ID is empty.')

    def parse_overview_page(self, response):

        movies = response.xpath(self.xpath_dict['movie_box'])
        if not movies or len(movies) == 0:
            self.logger.error("Cannot find movies boxes. Possibly a change in DOM or scraper got blocked!, please contact the developer.")
        else:
            for movie in movies:

                title = movie.xpath(self.xpath_dict['title'])
                if title and len(title) > 0:
                    title = title.extract()
                    title = [x.strip() for x in title if x != '']
                    title = [x for x in title if x != '']
                    title = title[0].strip()

                movie_id = ''
                movie_url = movie.xpath(self.xpath_dict['url'])
                if movie_url and len(movie_url) > 0:
                    movie_url = urljoin(response.url, movie_url.extract()[0].strip())
                    movie_url = movie_url.replace('?ref_=adv_li_i', '')
                    movie_id = re.findall(self.regex_dict['movie_id'], movie_url)
                    movie_id = movie_id[0].strip()

                year = movie.xpath(self.xpath_dict['year'])
                if year and len(year) > 0:
                    year = year.extract()[0].replace('(', '').replace(')', '').strip()

                poster_url = movie.xpath(self.xpath_dict['poster'])
                if poster_url and len(poster_url) > 0:
                    poster_url = poster_url.extract()[0].strip()
                    big_poster_url = re.sub('V1_.+', 'V1_SY1000_CR0,0,674,1000_AL_.jpg', poster_url)

                certificate = movie.xpath(self.xpath_dict['certificate'])
                if certificate and len(certificate) > 0:
                    certificate = certificate.extract()[0].strip()

                runtime = movie.xpath(self.xpath_dict['runtime'])
                if runtime and len(runtime) > 0:
                    runtime = runtime.extract()[0].strip()

                genre = movie.xpath(self.xpath_dict['genre'])
                if genre and len(genre) > 0:
                    genre = genre.extract()[0].strip()

                rating = movie.xpath(self.xpath_dict['rating'])
                if rating and len(rating) > 0:
                    rating = rating.extract()[0].strip()

                plot = movie.xpath(self.xpath_dict['plot'])
                if plot and len(plot) > 0:
                    plot = plot.extract()[0].strip()

                stars = movie.xpath(self.xpath_dict['stars'])
                if stars and len(stars) > 0:
                    stars = stars.extract()
                    stars = '| '.join(stars)
                else:
                    skip = True
                    stars_list = []
                    stars = movie.xpath(self.xpath_dict['stars_failed'])
                    stars = stars.extract()
                    for star in stars:
                        if 'Stars' in star:
                            skip = False
                        if not skip and '\n' not in star:
                            stars_list.append(star)
                    stars = stars_list

                votes = movie.xpath(self.xpath_dict['votes'])
                if votes and len(votes) > 0:
                    votes = votes.extract()
                    votes = [x.strip() for x in votes if 'Votes' not in x]
                    votes = votes[0]

                movie = {'id': movie_id,
                         'title': title,
                         'year': year,
                         'certificate': certificate,
                         'runtime': runtime,
                         'genre': genre,
                         'rating': rating,
                         'plot': plot,
                         'stars': stars,
                         'votes': votes,
                         'url': movie_url,
                         'poster_url': poster_url,
                         'big_poster_url': big_poster_url, }

                apify.pushData(movie)

        if not self.testing:
            next_ov = response.xpath(self.xpath_dict['next_overview'])
            if next_ov and len(next_ov) > 0:
                next_ov = next_ov.extract()[0].strip()
                next_ov = urljoin(response.url, next_ov)
                yield Request(url=next_ov,
                              headers=self.headers,
                              callback=self.parse_overview_page)