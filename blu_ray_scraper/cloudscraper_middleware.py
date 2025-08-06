# mobygames_scraper/middlewares/cloudscraper_middleware.py

import cloudscraper
from scrapy.http import HtmlResponse, Response


class CloudScraperMiddleware:
    
    def __init__(self):
        self.scraper = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows'}
        )

    def process_request(self, request, spider):
        # Only intercept HTTP GET requests
        if request.url.endswith((".jpg", ".png", ".jpeg", ".gif", ".bmp", ".webp")):
            return None  # Let Scrapy handle these

        if request.meta.get('zyte'):
            # Skip cloudscraper for Zyte proxy requests
            spider.logger.debug(f"Skipping Cloudscraper for: {request.url}")
            return None

        if request.method != "GET":
            return None

        cookies = request.cookies if request.cookies else None

        spider.logger.info(f"Cloudscraper fetching: {request.url}")
        # response = self.scraper.get(request.url, cookies=self.cookies)
        response = self.scraper.get(request.url)
        content_type = response.headers.get("Content-Type", "").lower()
        body = response.content
        

        if "image" in content_type or request.url.endswith((".jpg", ".png", ".jpeg", ".gif", ".bmp", ".webp")):
            return Response(
                url=request.url,
                status=response.status_code,
                body=body,
                request=request,
            )
        else:
            return HtmlResponse(
                url=request.url,
                status=response.status_code,
                body=body,
                encoding='utf-8',
                request=request,
            )
