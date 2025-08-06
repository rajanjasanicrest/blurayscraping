# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from scrapy.http import Request
from urllib.parse import urlparse
from io import BytesIO
import os
import re
from threading import Thread
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from botocore.exceptions import BotoCoreError, ClientError
from scrapy.exceptions import DropItem
from botocore.exceptions import ReadTimeoutError
import time
import logging

class BluRayScraperPipeline:
    def process_item(self, item, spider):
        return item
    
def sanitize_filename(title):
    sanitized_title = re.sub(r'[^\w\s]', '', title)
    sanitized_title = re.sub(r'\s+', ' ', sanitized_title).strip()
    return sanitized_title.replace(' ', '_')

def infer_image_key_from_url(original_url):
    keys = ['slipback_url', 'slip_url', 'overview_url', 'back_url', 'front_url']
    for key in keys:
        if key.split('_')[0] in original_url:
            return key
    return None  # fallback if no match

class PictureImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        # Collect image URLs from known keys
        for key in ['front_url', 'overview_url', 'back_url', 'slip_url', 'slipback_url']:
            url = item.get(key)
            if url:
                yield Request(url, meta={
                    'image_key': key,
                    'original_url': url,
                    'item_data': {
                        'title': item.get('title', 'unknown'),
                        'year': item.get('releaseYear', 'unknown'),
                        'blu_ray_url': item.get('blu_ray_url', '')
                    }
                })


    def file_path(self, request, response=None, info=None, *, item=None):
        meta = request.meta.get('item_data', {})
        title = meta.get('title', 'unknown')
        year = meta.get('year', 'unknown')
        image_key = request.meta.get('image_key')
        
        blu_ray_url = meta.get('blu_ray_url', '')
        slug = blu_ray_url.split('/')[-2] if blu_ray_url else 'unknown'
        image_name = f'{slug}_{image_key.split("_")[0]}.jpg'

        return f'DVD/{year}/{sanitize_filename(title)}/{image_name}'

    def item_completed(self, results, item, info):
        for success, result in results:
            if success:
                original_url = result['url']
                s3_path = result['path']
                s3_url = f'https://salient-blu-ray-scrapping.s3.amazonaws.com/{s3_path}'
                image_key = infer_image_key_from_url(original_url)

                if image_key:
                    item[image_key] = s3_url
                else:
                    info.spider.logger.warning(f"Image key not found for downloaded image: {original_url}")
            else:
                info.spider.logger.warning(f"Image failed to download for item: {item.get('title')}")
        return item


class ScreenshotImagesPipeline(ImagesPipeline):
    
    def process_item(self, item, spider):
        try:
            return super().process_item(item, spider)
        except ReadTimeoutError as e:
            spider.logger.warning(f"Skipping item due to S3 timeout: {item['image_url']}")
            return None  # or mark item as failed
        except Exception as e:
            spider.logger.error(f"An unexpected error occurred while processing {item['image_url']}: {e}")
            raise DropItem(f"Failed to process {item['image_url']} due to an error: {e}")


    def get_media_requests(self, item, info):
        for url in item.get('screenshot_urls', []):
            base_name = self.get_base_filename(url)
            fallbacks = self.get_fallback_versions(url)
            
            # Save the fallback list (excluding first) in the request meta
            yield Request(
                url=fallbacks[0],
                meta={
                    'original_url': url,
                    'base_name': base_name,
                    'title': item['title'],
                    'year': item['releaseYear'],
                    'fallbacks': iter(fallbacks[1:])  # <== store fallback iterator here
                },
                dont_filter=True
            )

    def store_file(self, path, buf, info, meta=None, headers=None):
        retries = 5
        for attempt in range(1, retries + 1):
            try:
                return super().store_file(path, buf, info, meta=meta, headers=headers)
            except (BotoCoreError, ClientError) as e:
                logging.warning(f"S3 upload failed (attempt {attempt}) for file: {path}. Error: {e}")
                if attempt == retries:
                    logging.error(f"Failed to upload {path} after {retries} attempts. Raising DropItem.")
                    raise DropItem(f"Failed to upload {path} after {retries} attempts.") from e 
                time.sleep(2 ** attempt)  # exponential backoff

    def get_base_filename(self, url):
        filename = os.path.basename(urlparse(url).path)
        match = re.match(r'(.*?)(?:(_1080p|_large|_tn))?\.jpg$', filename)
        return match.group(1) if match else filename

    def get_fallback_versions(self, url):
        versions = [url]
        if '_1080p' in url:
            versions.append(url.replace('_1080p', '_large'))
            versions.append(url.replace('_1080p', ''))
            versions.append(url.replace('_1080p', '_tn'))
        elif '_large' in url:
            versions.append(url.replace('_large', ''))
            versions.append(url.replace('_large', '_tn'))
        else:
            versions.append(url.replace('.jpg', '_tn.jpg'))
        return versions

    def media_failed(self, failure, request, info):
        fallback_iter = request.meta.get('fallbacks')
        try:
            next_url = next(fallback_iter)
            request.meta['fallbacks'] = fallback_iter  # reassign in case it's reused
            return Request(
                url=next_url,
                meta=request.meta,
                dont_filter=True
            )
        except StopIteration:
            info.spider.logger.warning(f"❌ All fallbacks failed for {request.meta.get('base_name')}")
            return None


    def file_path(self, request, response=None, info=None, item=None):
        title = request.meta.get('title', 'unknown')
        year = request.meta.get('year', 'unknown')
        filename = os.path.basename(urlparse(request.url).path)
        return f'DVD/{year}/{sanitize_filename(title)}/{filename}'

    def item_completed(self, results, item, info):
        chosen_urls = []

        for ok, data in results:
            if ok:
                iamge_failed = isinstance(data, Request)
                if iamge_failed:
                    info.spider.logger.warning(f"❌ Image download failed for {data.url}")
                    continue
                file_path = data['path']
                s3_url = f'https://salient-blu-ray-scrapping.s3.amazonaws.com/{file_path}'
                chosen_urls.append(s3_url)
            else:
                self.logger.warning("Download Failed")

        item['screenshot_urls'] = chosen_urls
        return item
