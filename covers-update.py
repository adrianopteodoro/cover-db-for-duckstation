import os
import logging
import warnings

import scrapy
from scrapy import Field, Item
from scrapy.crawler import CrawlerProcess
from scrapy.pipelines.images import ImagesPipeline
from scrapy.http import Request
from scrapy.utils.defer import maybe_deferred_to_future

warnings.filterwarnings("ignore", category=scrapy.exceptions.ScrapyDeprecationWarning)

logger = logging.getLogger("covers-update")

COVERS_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/covers"


class CoverImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        return [
            Request(x, meta={"serial": item["serial"]})
            for x in item.get("image_urls", [])
        ]

    def file_path(self, request, response=None, info=None, *, item=None):
        cover_serial = request.meta["serial"]
        return f"{cover_serial}.jpg"


class CoverImageItem(Item):
    serial = Field()
    image_urls = Field()
    images = Field()


class PsxDataCenterCoverSpider(scrapy.Spider):
    name = "psxdatacentercover"
    start_urls = [
        "https://psxdatacenter.com/ulist.html",
        "https://psxdatacenter.com/plist.html",
        "https://psxdatacenter.com/jlist.html",
    ]

    def parse_item(self, serial: str, image_urls: list) -> CoverImageItem:
        return CoverImageItem(
            serial=serial,
            image_urls=image_urls,
        )

    async def parse(self, response):
        if response.url in self.start_urls:
            for href in response.xpath('//*/tr/td[@class="col1"]/a/@href'):
                href_data = href.get()
                if (
                    not response.urljoin(href_data) == response.url
                    or response.urljoin(href_data) in self.start_urls
                ):
                    yield response.follow(href_data, self.parse)
        else:
            game_serials = response.xpath(
                '//*[@id="table7"]/tr[2]/td[@class="darkcell"]/text()'
            ).extract()
            extracted_cover_hires_image = response.xpath(
                '//*[@id="table28"]/tr[3]/td[1]/a/@href'
            ).get()
            extracted_cover_lowres_image = response.xpath(
                '//*[@id="table2"]/tr[2]/td[1]/img/@src'
            ).get()
            cover_image_urls = []
            if extracted_cover_hires_image and "-F-ALL" in extracted_cover_hires_image and ".html" in extracted_cover_hires_image:
                additional_request = Request(response.urljoin(extracted_cover_hires_image))
                deferred = self.crawler.engine.download(additional_request)
                additional_response = await maybe_deferred_to_future(deferred)
                hires_image = additional_response.xpath("//*/p[3]/*/img/@src").get()
                if hires_image:
                    cover_image_urls = [additional_response.urljoin(hires_image)]
            if not cover_image_urls:
                cover_image_urls = [response.urljoin(extracted_cover_lowres_image)] if extracted_cover_lowres_image else []
            for serial in game_serials:
                if cover_image_urls and not (
                    "https://psxdatacenter.com/images/covers/none.jpg"
                    in cover_image_urls
                ):
                    yield self.parse_item(serial, cover_image_urls)


process = CrawlerProcess(
    settings={
        "FEEDS": {
            "last_run.json": {
                "format": "json",
                "fields": ["serial", "image_urls"],
                "item_classes": [CoverImageItem],
                "overwrite": True,
            },
        },
        "IMAGES_STORE": "covers",
        "ITEM_PIPELINES": {CoverImagesPipeline: 1},
        "LOG_LEVEL": "INFO",
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_FAIL_ON_DATALOSS": False,
        "RETRY_ENABLED": True,
    }
)

process.crawl(PsxDataCenterCoverSpider)
process.start()
