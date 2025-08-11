import os
import re
import math
import json
import scrapy
import requests
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
from scrapy.exceptions import CloseSpider
# from getMovieList import getMovieList  # Assumes br works with BeautifulSoup or Scrapy-compatible page
from scrapy.exceptions import CloseSpider
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file
from scrapy.utils.project import get_project_settings

def clean_text(text):
    """Remove special characters and extra spaces from the title."""
    return re.sub(r'[^a-zA-Z0-9 ]', '', text).strip().lower()


def is_title_match(target_title, ebay_title, threshold=80):
    # Remove special characters and convert to lowercase
    def clean_text(text):
        return re.sub(r'[^a-zA-Z0-9\s]', '', text).lower()

    target_words = clean_text(target_title).split()
    ebay_words = clean_text(ebay_title).split()

    # Count matching words
    match_count = sum(1 for word in target_words if word in ebay_words)
    match_percentage = (match_count / len(target_words)) * 100 if target_words else 0

    return match_percentage >= threshold, match_percentage

def extract_image_urls(response):
    # Combine all <script> tags' inner text into one string
    script_texts = response.xpath('//script/text()').getall()
    script_text = "\n".join(script_texts)

    image_urls = {
        "front_url": None,
        "overview_url": None,
        "back_url": None,
        "slip_url": None,
        "slipback_url": None
    }

    # Define URL patterns
    patterns = {
        "front_url": r"https://images\.static-bluray\.com/movies/covers/\d+_front\.jpg\?t=\d+",
        "overview_url": r"https://images\.static-bluray\.com/movies/covers/\d+_overview\.jpg\?t=\d+",
        "back_url": r"https://images\.static-bluray\.com/movies/covers/\d+_back\.jpg\?t=\d+",
        "slip_url": r"https://images\.static-bluray\.com/movies/covers/\d+_slip\.jpg\?t=\d+",
        "slipback_url": r"https://images\.static-bluray\.com/movies/covers/\d+_slipback\.jpg\?t=\d+"
    }

    # Extract and categorize URLs
    for key, pattern in patterns.items():
        match = re.search(pattern, script_text)
        if match:
            image_urls[key] = match.group(0)

    # Store URLs in movie_details
    return image_urls


class BluRaySpider3D(scrapy.Spider):
    name = "bluray_3d"
    series = '3D'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0',
        'DOWNLOAD_DELAY': 1.0,
        # Add proxy or middleware settings here if needed
    }

    def __init__(self, country, year=2019, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        os.makedirs("data", exist_ok=True)
        self.years = list(range(2006, 2025))
        self.years.append(1969)
        # self.year = int(year) if year else 2019
        self.base_data_path = "data"
        # self.movies_list_path = f'dvd_list/{self.year}-list.json'
        self.year = str(year)
        self.country = country
        print('-------')
        print(country)
        print('-------')
        try:        
            with open(f'data/3D-{self.country}.json', 'r', encoding='utf-8') as f:
                self.existing_data = json.load(f)
        except FileNotFoundError:
            self.existing_data = []
        
        if self.existing_data:
            self.existing_data = [item['blu_ray_url'] for item in self.existing_data]
        

    def start_requests(self):
        for year in self.years:
            url = f'https://www.blu-ray.com/movies/search.php?releaseyear={year}&other_bluray3d=1&submit=Search&action=search&page=0'
            
            yield scrapy.Request(
                url=url,
                callback=self.parse_movie_list,
                meta={'page':0, 'year': year},
                cookies={
                    "country": self.country,
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )
    
    def parse_movie_list(self, response):
        if response.status == 403:
            if not hasattr(self, 'closed_due_to_403'):
                self.closed_due_to_403 = True
            if not self.closed_due_to_403:
                self.closed_due_to_403 = True
                self.logger.warning("403 Forbidden received. Closing spider.")
                raise CloseSpider(reason="403 Forbidden")
            return

        if not response.xpath('//a[@href="https://www.blu-ray.com/"]'):
            raise CloseSpider(reason="IP blocked or blank page")

        page = response.meta["page"]
        year = response.meta["year"]
        movie_links = response.xpath('//table[@class="bevel"]//a[contains(@href, "/movies/")]/@href').getall()
        
        # Track processed URLs to avoid duplicates
        if not hasattr(self, 'processed_urls'):
            self.processed_urls = set()
        
        unique_links = []
        for link in movie_links:
            absolute_url = response.urljoin(link)
            if absolute_url not in self.processed_urls and absolute_url not in self.existing_data:
                unique_links.append(absolute_url)
                self.processed_urls.add(absolute_url)

        self.logger.info(f"Page {page}: Found {len(movie_links)} total links, {len(unique_links)} unique new links")

        for absolute_url in unique_links:
            yield scrapy.Request(
                url=absolute_url,
                callback=self.parse_movie_detail,
                meta={'movie_url': absolute_url, 'year': year},
                cookies={
                    "country": 'us',
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )

        # Only calculate pagination on first page
        if page == 0:
            total_text = response.css(".oswaldcollection::text").get()
            match = re.search(r'\d+', total_text)
            if match:
                movies_number = int(match.group())
                total_pages = math.ceil(movies_number / 20)
                self.logger.info(f"{movies_number} movies to scrape for year {year} across {total_pages} pages.")

                # Start from page 1 instead of page 2, or check what the actual pagination scheme is
                for page_no in range(1, total_pages):  # Changed from range(2, total_pages + 1)
                    url = f"https://www.blu-ray.com/movies/search.php?releaseyear={year}&other_bluray3d=1&submit=Search&action=search&page={page_no}"
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_movie_list,
                        meta={"page": page_no, 'year': year},
                        cookies={
                            "country": self.country,
                            "listlayout_7": "simple",
                            "listlayout_21": "simple",
                        }
                    )
        
        # Alternative: Check if this page has results, if not, stop pagination
        elif len(movie_links) == 0:
            self.logger.info(f"No more results found on page {page}, stopping pagination")
            return


    def parse_movie_detail(self, response):
        year = response.meta['year']
        if response.status == 403:
            if not self.closed_due_to_403:
                self.closed_due_to_403 = True
                self.logger.warning("403 Forbidden received. Closing spider.")
                raise CloseSpider(reason="403 Forbidden")
            return
        elif response.status == 404:
            yield {
                "blu_ray_url": response.url
            }
            return
        if not response.xpath('//a[@href="https://www.blu-ray.com/"]'):
            raise CloseSpider(reason="IP blocked or blank page")

        movie_href = response.url
        # year = self.year

        blu_ray_id = movie_href.split('/')[-2]
        movie_details = {
            'releaseYear': year,
            'blu_ray_url': movie_href,
            'missing_links': False
        }

        core_info = response.css('span.subheading.grey ::text').getall()
        core_info = ' '.join(t.strip() for t in core_info if t.strip())
        core_texts = [t.strip() for t in core_info.split('|')] if core_info else []

        movie_details["production"] = ""
        for text in core_texts:
            if re.fullmatch(r"\d{4}(-\d{4})?", text):
                movie_details["production_year"] = text
            elif 'min' in text:
                movie_details["runtime"] = text
            elif 'rated' in text.lower():
                movie_details["age_rating"] = text
            elif re.fullmatch(r"[A-Za-z]+ \d{2}, \d{4}", text):
                movie_details["release_date"] = text
            elif not movie_details["production"]:
                movie_details["production"] = text

        # Technical Specs
        td = response.xpath("//td[@width='228px']").get()
        if td:
            specs_html = scrapy.Selector(text=td)
            # Remove <br> tags from specs HTML
            specs_html = scrapy.Selector(text=td.replace('<br>', '\n'))
            headers = specs_html.css(".subheading *::text").getall()
            n = len(headers)
            for i in range(n):
                current = headers[i]
                next_header = headers[i + 1] if i + 1 < n else None
                raw_section = self.get_text_between(specs_html, current, next_header)

                if current in ['Video']:
                    for line in raw_section.split('\n'):
                        if 'Codec' in line:
                            movie_details['codec'] = line.split(':', 1)[-1].strip()
                        elif 'Encoding' in line:
                            movie_details['encoding'] = line.split(':', 1)[-1].strip()
                        elif 'Resolution' in line:
                            movie_details['resolution'] = line.split(':', 1)[-1].strip()
                        elif 'Aspect ratio' in line:
                            movie_details['aspect_ratio'] = line.split(':', 1)[-1].strip()
                        elif 'Original aspect ratio' in line:
                            movie_details['original_aspect_ratio'] = line.split(':', 1)[-1].strip()
                elif current in ['Discs', 'Disc']:
                    movie_details['discs'] = raw_section.split('\n')
                elif current == 'Playback':
                    movie_details['playback'] = raw_section.split('\n')
                elif current == 'Packaging':
                    movie_details['packaging'] = raw_section.split('\n')

        # Audio
        audio_lines = response.css('div#longaudio::text').getall()
        audio_text = ", ".join([line.strip() for line in audio_lines if line.strip()])
        movie_details['audio'] = audio_text.replace(', (, )', '').replace('("less")', '').replace('(, )', '')

        # Subtitles
        subs = response.css('div#longsubs::text').getall()
        subs_text = ", ".join([s.strip() for s in subs if s.strip()])
        movie_details['subtitles'] = subs_text.replace(', (, )', '').replace('("less")', '').replace('(, )', '')

        # Pricing
        pricing_td_html = response.xpath("//td[@width='266px']").get()
        if pricing_td_html:
            pricing_selector = scrapy.Selector(text=pricing_td_html)
            pricing_text = self.get_text_between(pricing_selector, "Price", "Price")
            for line in pricing_text.split('\n'):
                if 'Used' in line:
                    movie_details['used_price'] = line.split("$")[-1].strip().split()[0]
                elif 'New' in line:
                    movie_details['new_price'] = line.split("$")[-1].strip().split()[0]

        # Titles, Description, Cast
        movie_details['title'] = response.css("#movie_info h3::text").get(default="").strip()
        movie_details['subheading_title'] = response.css('.subheadingtitle::text').get(default="").strip()

        info_html = response.xpath("//div[@id='movie_info']").get()
        # info_element = scrapy.Selector(text=info_html.replace('<br>', '\n') )
        info_text = response.css("#movie_info *::text").getall()
        # info_text = 
        info_text = [line.strip() for line in info_text if line.strip() and "Screenshots" not in line]

        description_lines = []
        for line in info_text[1:]:
            if any(x in line for x in ["Directors:", "Producers:", "Starring:", "Writers:", "Narrator:", "Director:", "Producer:"]):
                break
            description_lines.append(line)
        movie_details["description"] = "\n".join(description_lines)

        # cast and crew
        cast_crew_page_url = response.xpath("//a[contains(@href, '#Castandcrew')]/@href").get()
        if cast_crew_page_url:
            pass
        else:
            movie_details['cast_and_crew2'] = {}
            key = None
            for line in info_text:
                changed_now = False
                if "Director:" in line or "Directors:" in line:
                    key = "Director"
                    changed_now = True
                elif "Starring:" in line:
                    key = "Cast"
                    changed_now = True
                elif "Writers:" in line or "Writer:" in line:
                    key = "Writer"
                    changed_now = True
                elif "Producers:" in line or "Producer:" in line:
                    key = "Producer"
                    changed_now = True
                elif "Narrator:" in line or "Narrators:" in line:
                    key = "Narrator"
                    changed_now = True
                elif "Composer:" in line or "Composers:" in line:
                    key = "Composer"
                    changed_now = True
                if key:
                    if changed_now: continue
                    if '»' in line: continue
                    if 'cast & crew' in line: continue
                    if ',' in line: continue
                    if ':' in line: continue
                    if key in movie_details['cast_and_crew2']:
                        movie_details['cast_and_crew2'][key].append({line.split(":", 1)[-1].strip()})
                    else:
                        movie_details['cast_and_crew2'][key] = [line.split(":", 1)[-1].strip()]

        # Genres
        genres = response.css('.genreappeal *::text').getall()[:3]
        genres = [g.strip() for g in genres if g.strip()]
        movie_details['genres'] = genres

        # Amazon ID
        amzn_link = response.css("#movie_buylink::attr(href)").get()
        ebay_link = response.css("a[href*='/sch/i.html?_nkw=']::attr(href)").get()
        if ebay_link:
            upc = movie_details.get('upc') or parse_qs(urlparse(ebay_link).query).get('_nkw', [''])[0]
            movie_details['upc'] = upc

        # images:
        image_urls = extract_image_urls(response)
        movie_details.update(**image_urls)

        # screenshots:
        # Now go to screenshots page
        screenshots_section = response.xpath('//a[contains(@href, "#Screenshots")]/@href').get()
        if screenshots_section:
            screenshots_url = response.urljoin(screenshots_section)
            yield scrapy.Request(
                url=screenshots_url,
                callback=self.parse_screenshots,
                meta={'blu_ray_id': blu_ray_id, 'movie_details': movie_details, 'amazon_link': amzn_link, 'ebay_link': ebay_link, 'image_urls': image_urls, "screenshot_page": True, 'cast_crew_page_url': cast_crew_page_url},
                dont_filter=True,
                    cookies={
                    "country": 'us',
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )
        else:
            yield scrapy.Request(
                url=response.url,
                callback=self.parse_screenshots,
                meta={'blu_ray_id':blu_ray_id, 'movie_details': movie_details, 'amazon_link': amzn_link, 'ebay_link': ebay_link, 'image_urls': image_urls, "screenshot_page": False, 'cast_crew_page_url': cast_crew_page_url},
                dont_filter=True,
                cookies={
                    "country": 'us',
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )

    def parse_screenshots(self, response):
        if response.status == 403:
            if not self.closed_due_to_403:
                self.closed_due_to_403 = True
                self.logger.warning("403 Forbidden received. Closing spider.")
                raise CloseSpider(reason="403 Forbidden")
            return
        movie_details = response.meta['movie_details']
        blu_ray_id = response.meta['blu_ray_id']
        
        if not response.xpath('//a[@href="https://www.blu-ray.com/"]'):
            raise CloseSpider(reason="IP blocked or blank page")

        title = movie_details.get("title")
        screenshot_page = response.meta["screenshot_page"]
        screenshot_urls = []
        if screenshot_page:
            img_urls = list({
                img.attrib["src"]
                for img in response.xpath('//img[contains(@src, "/reviews/")]')
                if "_tn" not in img.attrib["src"]
            })

            script_urls = list(set(re.findall(r'src\s*[:=]\s*[\'"]([^\'"]*/reviews/[^\'"]+)[\'"]', response.text)))
            img_urls = list(set(img_urls + script_urls))


            for url in img_urls:
                if url:
                    if '_tn' in url:
                        # Replace _tn with _1080p
                        url = url.replace('_tn', '_1080p')
                        screenshot_urls.append(url)  # In case there is no extension
                    elif '_large' in url:
                        # Replace _large with _1080p
                        url = url.replace('_large', '_1080p')
                        screenshot_urls.append(url)  # In case there is no extension
                    else:
                        # Split the URL before the extension
                        parts = url.rsplit('.', 1)
                        if len(parts) == 2:
                            # Append _1080p before the extension
                            new_url = f'{parts[0]}_1080p.{parts[1]}'
                            screenshot_urls.append(new_url)
                        else:
                            screenshot_urls.append(url)  # In case there is no extension
        else:
            img_urls = list({
                img.attrib["src"]
                for img in response.xpath('//img[contains(@src, "/reviews/")]')
            })
            script_urls = list(set(re.findall(r'src\s*[:=]\s*[\'"]([^\'"]*/reviews/[^\'"]+)[\'"]', response.text)))
            img_urls = set(img_urls + script_urls)
            for url in img_urls:
                if url:
                    url = url.replace('_tn', '_1080p')
                    screenshot_urls.append(url)

        for i, url in enumerate(screenshot_urls):
            if '/images/reviews/' in url:
                url = url.replace('.jpg', '_1080p.jpg') if '_1080p' not in url else url
                screenshot_urls[i] = f'https://www.blu-ray.com{url}'

        screenshot_urls = [url for url in screenshot_urls if '1158_2' not in url and '1158_3' not in url]
        screenshot_urls = list(set(screenshot_urls))


        movie_details['screenshot_urls'] = screenshot_urls

        if cast_crew_page_url := response.meta.get('cast_crew_page_url'):
            yield scrapy.Request(
                url=f'https://www.blu-ray.com/movies/movies.php?id={blu_ray_id}&action=showcastandcrew&page=',
                callback=self.parse_cast_and_crew,
                meta={'movie_details': movie_details, 'amazon_link': response.meta['amazon_link'], 'ebay_link': response.meta['ebay_link'], 'image_urls': response.meta['image_urls']},
                dont_filter=True,
                cookies={
                    "country": 'us',
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )
        else:
            amzn_link = response.meta['amazon_link']
            ebay_link = response.meta['ebay_link']
            image_urls = response.meta['image_urls']
            if amzn_link:
                try:
                    final_url = requests.get(amzn_link, timeout=100).url
                    amazon_id = final_url.split("?")[0].split("/")[-1]
                    movie_details["amazon_id"] = amazon_id
                    yield scrapy.Request(
                        url=f'https://camelcamelcamel.com/product/{amazon_id}',
                        callback=self.parse_camelcamelcamel,
                        meta={
                            'movie_details': movie_details, 
                            'amazon_link': amzn_link, 
                            'ebay_link': ebay_link, 
                            'image_urls': image_urls,
                            'proxy': f'http://{os.getenv('ZYTE_KEY')}:@api.zyte.com:8011',
                            # 'browserHtml': True 
                        },
                        dont_filter=True
                    )
                    
                except Exception as e:
                    self.logger.warning({e})
    
    def parse_cast_and_crew(self, response):
        if response.status == 403:
            if not self.closed_due_to_403:
                self.closed_due_to_403 = True
                self.logger.warning("403 Forbidden received. Closing spider.")
                raise CloseSpider(reason="403 Forbidden")
            return
        movie_details = response.meta['movie_details']
        amzn_link = response.meta['amazon_link']
        ebay_link = response.meta['ebay_link']
        image_urls = response.meta['image_urls']

        cast_crew_data = defaultdict(list)
        # Loop through all tables under the container
        for table in response.css('table.bevel'):
            
            # Get the role (Director, Writer, etc.)
            role = table.css("td:nth-child(2) h5::text").get()
            if not role:
                continue

            # Get all names in the table
            for row in table.css("tr"):
                name = row.css("td.middle a::text").get()
                if name:
                    cast_crew_data[role].append(name)

        movie_details.update( {'cast_and_crew': dict(cast_crew_data)} )
        if amzn_link:
            try:
                final_url = requests.get(amzn_link, timeout=100).url
                amazon_id = final_url.split("?")[0].split("/")[-1]
                movie_details["amazon_id"] = amazon_id
                yield scrapy.Request(
                    url=f'https://camelcamelcamel.com/product/{amazon_id}',
                    callback=self.parse_camelcamelcamel,
                    meta={
                        'movie_details': movie_details, 
                        'amazon_link': amzn_link, 
                        'ebay_link': ebay_link, 
                        'image_urls': image_urls,
                        'proxy': f'http://{os.getenv('ZYTE_KEY')}:@api.zyte.com:8011',
                        # 'browserHtml': True 
                    },
                    dont_filter=True
                )
                
            except Exception as e:
                self.logger.warning({e})
        else:
            upc = movie_details.get('upc', None)
            if upc: 
                yield scrapy.Request(
                    url=f'https://www.ebay.com/sch/i.html?_nkw={upc}',
                    callback=self.parse_epid_results,
                    meta={
                        'movie_details': movie_details, 
                        'target_title': movie_details['title'], 
                        'max_results': 4,
                        'proxy': f'http://{os.getenv('ZYTE_KEY')}:@api.zyte.com:8011',
                    },
                    dont_filter=True
                )
            else:
                yield movie_details

    def parse_camelcamelcamel(self, response):
        
        movie_details = response.meta['movie_details']
        amzn_link = response.meta['amazon_link']
        ebay_link = response.meta['ebay_link']
        image_urls = response.meta['image_urls']

        """
        Scrapy callback to parse product identifiers and price details from CamelCamelCamel.
        """
        product_details = {}

        # table with product identifiers
        tables = response.xpath("//table[@class='product_fields']")

        if tables:
            rows = tables[0].xpath(".//tr")
            for row in rows:
                key = row.xpath("./td[1]//text()").getall()
                key = ''.join(key).replace('\u200b', '').strip()

                value_parts = row.xpath("./td[2]//text()").getall()
                value = ''.join(value_parts).replace('\u200b', '').strip()
                key_mappings = {
                    'manufacturer': 'Manufacturer',
                    'isbn': 'ISBN',
                    'ean': 'EAN',
                    'upc': 'UPC',
                    'sku': 'SKU',
                    'asin': 'ASIN'
                }
                for key_pattern, standard_key in key_mappings.items():
                    if standard_key in key:
                        if value:
                            product_details[key_pattern] = value
                        break
        else:
            self.logger.warning("Product details table not found")

        
        # price table
        rows = response.xpath("//div[@class='table-scroll camelegend']//table//tr")

        for row in rows:
            label = row.xpath(".//td[1]/text()").getall()
            if not label:
                continue
            label = [t.strip() for t in label if t.strip()][0].lower()

            # Extract current and average prices from the appropriate columns
            current_price = row.xpath("./td[4]/text()[1]").get(default="").strip()
            average_price = row.xpath("./td[5]/text()[1]").get(default="").strip()

            if "amazon" in label:
                product_details["amazon_current_price"] = current_price.replace('$','') if current_price != "-" else '-'
                product_details["amazon_average_price"] = average_price.replace('$','') if average_price != "-" else '-'
            elif "3rd party used" in label:
                product_details["third_used_current_price"] = current_price.replace('$','') if current_price != "-" else '-'
                product_details["third_used_average_price"] = average_price.replace('$','') if average_price != "-" else 'None'


        for k in ['upc', 'manufacturer', 'isbn', 'ean', 'sku']:
            if k == 'upc':
                if k not in product_details: continue
            movie_details[k.lower()] = product_details.get(k, '')

        movie_details.update({
            'amazon_current_price': product_details.get('amazon_current_price', '-'),
            'amazon_average_price': product_details.get('amazon_average_price', '-'),
            'third_used_current_price': product_details.get('third_used_current_price', '-'),
            'third_used_average_price': product_details.get('third_used_average_price', '-'),
        })

        upc = movie_details.get('upc', None)
        if upc: 
            yield scrapy.Request(
                url=f'https://www.ebay.com/sch/i.html?_nkw={upc}',
                callback=self.parse_epid_results,
                meta={
                    'movie_details': movie_details, 
                    'target_title': movie_details['title'], 
                    'max_results': 4,
                    'proxy': f'http://{os.getenv('ZYTE_KEY')}:@api.zyte.com:8011',
                },
                dont_filter=True
            )
        else:
            yield movie_details

    def parse_epid_results(self, response):
        movie_details = response.meta["movie_details"]
        target_title = response.meta["target_title"]
        max_results = response.meta.get("max_results", 10)

        title_clean = clean_text(target_title)

        items = response.css("ul.srp-results > li.s-item")[:max_results]
        for item in items[:3]:  # Limit to first 3 for efficiency
            ebay_title = item.css('.s-item__title > span::text').get()
            if not ebay_title:
                continue  

            match, confidence = is_title_match(title_clean, ebay_title)
            if not match:
                continue

            product_url = item.css("a.s-item__link::attr(href)").get()
            if not product_url:
                continue

            query = urlparse(product_url).query
            epid = parse_qs(query).get("epid", [None])[0]
            
            if epid:
                movie_details["epid"] = epid
                break

        yield movie_details

    def get_text_between(self, selector, start_text, end_text=None):
        text = selector.xpath("string()").get()
        if not text:
            return ""
        start_idx = text.find(start_text)
        if start_idx == -1:
            return ""
        end_idx = text.find(end_text, start_idx + len(start_text)) if end_text else -1
        return text[start_idx + len(start_text):end_idx].strip() if end_idx != -1 else text[start_idx + len(start_text):].strip()
        
    # def closed(self, reason):
    #     self.dump_data()
    #     self.logger.info(f"Finished scraping year {self.year}")

    # def dump_data(self):
    #     with open(self.output_file, 'w', encoding='utf-8') as f:
    #         json.dump(self.existing_data, f, indent=4, ensure_ascii=False)