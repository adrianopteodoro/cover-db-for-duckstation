import re
import os
import logging
import warnings
import grequests

import scrapy
import requests
from scrapy import Field, Item, Request
from scrapy.crawler import CrawlerProcess
from scrapy.pipelines.images import ImagesPipeline

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

    def parse(self, response):
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
            if extracted_cover_hires_image:
                extracted_cover_hires_image_jpg = extracted_cover_hires_image.replace(
                    ".html", ".jpg"
                )
                extracted_cover_hires_image_png = extracted_cover_hires_image.replace(
                    ".html", ".png"
                )
                rs = (
                    grequests.get(url)
                    for url in [
                        response.urljoin(extracted_cover_hires_image_jpg),
                        response.urljoin(extracted_cover_hires_image_png),
                    ]
                )
                cover_image_urls = [
                    res.url for res in grequests.imap(rs) if res.status_code == 200
                ]
            else:
                extracted_cover_lowres_image = response.xpath(
                    '//*[@id="table2"]/tr[2]/td[1]/img/@src'
                ).get()
                if extracted_cover_lowres_image:
                    cover_image_urls = [response.urljoin(extracted_cover_lowres_image)]
                else:
                    cover_image_urls = []
            for serial in game_serials:
                if (
                    cover_image_urls
                    and not "https://psxdatacenter.com/images/covers/none.jpg"
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
                "overwrite": False,
            },
        },
        "IMAGES_STORE": "covers",
        "DOWNLOAD_DELAY": 0.2,
        "ITEM_PIPELINES": {CoverImagesPipeline: 1},
        "LOG_LEVEL": "INFO",
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
    }
)

process.crawl(PsxDataCenterCoverSpider)
process.start()
