import os
import re
import math
import json
import scrapy
import requests
from collections import defaultdict
from urllib.parse import urlparse, parse_qs
from scrapy.exceptions import CloseSpider

# from getMovieList import getMovieList  # Assumes it works with BeautifulSoup or Scrapy-compatible page
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


class BluRaySpider(scrapy.Spider):
    name = "bluray_3d"
    series = '3D'

    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0',
        'DOWNLOAD_DELAY': 1.0,
        # Add proxy or middleware settings here if needed
    }


    def __init__(self, year=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.year = int(year) if year else 2023
        self.base_data_path = "data"
        self.movies_list_path = f'3dmovie_list/{self.year}-list.json'
        self.year = str(year)

        os.makedirs("data", exist_ok=True)

        # self.existing_data = self.load_existing_data()
        self.existing_data = []
        # self.scraped_urls = {x['blu_ray_url'] for x in self.existing_data if x}
        # self.movies_list = self.load_or_generate_movie_list()

    def start_requests(self):
        if self.series == '3D':
            url = f'https://www.blu-ray.com/movies/search.php?releaseyear={self.year}&other_bluray3d=1&submit=Search&action=search&page=0'
        elif self.series == 'DVD':
            url = f'https://www.blu-ray.com/dvd/search.php?releaseyear={self.year}&&submit=Search&action=search&page=0'
        yield scrapy.Request(
            url=url,
            callback=self.parse_movie_list,
            meta={'page':0},
            cookies={
                "country": "uk",
                "listlayout_7": "simple",
                "listlayout_21": "simple",
            }
        )
    
    def parse_movie_list(self, response):
        page = response.meta["page"]
        movie_links = response.xpath('//table[@class="bevel"]//a[contains(@href, "/dvd/")]/@href').getall()

        print('here')
        for link in movie_links:
            print('there')
            absolute_url = response.urljoin(link)
            yield scrapy.Request(
                url=absolute_url,
                callback=self.parse_movie_detail,
                meta={'movie_url': absolute_url},
                cookies={
                    "country": "uk",
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )
        
        if page == 1:
            total_text = response.css(".oswaldcollection::text").get()
            match = re.search(r'\d+', total_text)
            if match:
                movies_number = int(match.group())
                total_pages = math.ceil(movies_number / 20)
                self.logger.info(f"{movies_number} movies to scrape for year {self.year} across {total_pages} pages.")

                for page_no in range(2, total_pages + 1):
                    url = f"{self.base_url}&page={page_no}"
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_movies_list,
                        meta={"page": page_no},
                        cookies={
                        "country": "uk",
                        "listlayout_7": "simple",
                        "listlayout_21": "simple",
                    }
                    )

    def parse_movie_detail(self, response):
        movie_href = response.url
        year = self.year

        blu_ray_id = movie_href.split('/')[-2]
        movie_details = {
            'releaseYear': year,
            'blu_ray_url': movie_href,
            'missing_links': False
        }

        core_info = response.css('.subheading.grey::text').get()
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
        if 'less' in audio_text: audio_text.replace('("less")', '') 
        movie_details['audio'] = audio_text

        # Subtitles
        subs = response.css('div#longsubs::text').getall()
        subs_text = ", ".join([s.strip() for s in subs if s.strip()])
        if 'less' in subs_text: subs_text.replace('("less")', '')
        movie_details['subtitles'] = subs_text

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
            print(cast_crew_page_url)
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
        ebay_link = response.css("a[href*='ebay.com/sch/']::attr(href)").get()
        if ebay_link:
            print('----------------------------------')
            print('here')
            print('----------------------------------')
            upc = movie_details.get('upc') or parse_qs(urlparse(ebay_link).query).get('_nkw', [''])[0]
            print(upc)
            movie_details['upc'] = upc

        # images:
        image_urls = extract_image_urls(response)
        movie_details['image_urls'] = image_urls

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
                    "country": "uk",
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
                    "country": "uk",
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )

    def parse_screenshots(self, response):
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
            print(cast_crew_page_url)
            yield scrapy.Request(
                url=f'https://www.blu-ray.com/movies/movies.php?id={blu_ray_id}&action=showcastandcrew&page=',
                callback=self.parse_cast_and_crew,
                meta={'movie_details': movie_details, 'amazon_link': response.meta['amazon_link'], 'ebay_link': response.meta['ebay_link'], 'image_urls': response.meta['image_urls']},
                dont_filter=True,
                cookies={
                    "country": "uk",
                    "listlayout_7": "simple",
                    "listlayout_21": "simple",
                }
            )
        else:
            amzn_link = response.meta['amazon_link']
            ebay_link = response.meta['ebay_link']
            image_urls = response.meta['image_urls']
            yield movie_details
            
    
    def parse_cast_and_crew(self, response):
        movie_details = response.meta['movie_details']
        amzn_link = response.meta['amazon_link']
        ebay_link = response.meta['ebay_link']
        image_urls = response.meta['image_urls']

        cast_crew_data = defaultdict(list)
        # Loop through all tables under the container
        for table in response.css('table.bevel'):
            
            # Get the role (Director, Writer, etc.)
            role = table.css("td:nth-child(2) h5::text").get()
            print(role)
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
                        'browserHtml': True 
                    },
                    dont_filter=True
                )
                
            except Exception as e:
                self.logger.warning({e})


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
        tables = response.xpath("//table[contains(@class, 'product_fields') or @id='product_fields']")
        if tables:
            rows = tables[0].xpath(".//tr")
            for row in rows:
                key = row.xpath("./td[1]//text()").get(default="").strip().lower().replace(':', '')
                value_parts = row.xpath("./td[2]//text()").getall()
                value = ''.join(value_parts).replace('\u200b', '').strip()

                key_mappings = {
                    'manufacturer': 'Manufacturer',
                    'isbn': 'ISBN',
                    'ean': 'EAN',
                    'upc': 'UPC_2',
                    'sku': 'SKU',
                    'asin': 'ASIN'
                }
                for key_pattern, standard_key in key_mappings.items():
                    if key_pattern in key:
                        if value:
                            product_details[standard_key] = value
                        break
        else:
            self.logger.warning("Product details table not found")

        # price table
        price_rows = response.xpath("//tbody/tr")
        for row in price_rows:
            category = row.xpath("./@data-field").get(default="").strip()
            prices = row.xpath("./td//text()").getall()
            clean_prices = [p.strip().replace('$', '').split('(')[0] for p in prices if p.strip()]

            if category == 'amazon' and len(clean_prices) >= 2:
                product_details['amazon_average_price'] = clean_prices[-2]
                product_details['amazon_current_price'] = clean_prices[-1]
            elif category == 'used' and len(clean_prices) >= 2:
                product_details['third_used_average_price'] = clean_prices[-2]
                product_details['third_used_current_price'] = clean_prices[-1]

        for k in ['UPC_2', 'Manufacturer', 'ISBN', 'EAN', 'SKU']:
            movie_details[k.lower()] = product_details.get(k, '')
        movie_details.update({
            'amazon_current_price': product_details.get('amazon_current_price', ''),
            'amazon_average_price': product_details.get('amazon_average_price', ''),
            'third_used_current_price': product_details.get('third_used_current_price', ''),
            'third_used_average_price': product_details.get('third_used_average_price', ''),
        })

        upc = movie_details['upc']
        if upc: 
            print('here')
            yield scrapy.Request(
                url=f'https://www.ebay.com/sch/i.html?_nkw={upc}',
                callback=self.parse_epid_results,
                meta={
                    'movie_details': movie_details, 
                    'target_title': movie_details['title'], 
                    'max_results': 4,
                    'proxy': f'http://{os.getenv('ZYTE_KEY')}:@api.zyte.com:8011',
                    'browserHtml': True 
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
            ebay_title = item.css(".s-item__title::text").get()
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

            movie_details["ebay_id"] = epid

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
