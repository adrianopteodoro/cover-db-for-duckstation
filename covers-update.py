import re
import os
import hashlib
import logging
import grequests

import scrapy
from scrapy import Field, Item, Request
from scrapy.crawler import CrawlerProcess
from scrapy.pipelines.images import ImagesPipeline
from io import BytesIO
from scrapy.utils.misc import md5sum

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
    checksum = Field()


class PsxDataCenterCoverSpider(scrapy.Spider):
    name = "psxdatacentercover"
    start_urls = [
        "https://psxdatacenter.com/images/covers/",
    ]

    def check_hires(self, hires_front_cover_urls: list):
        rs = (grequests.get(url) for url in hires_front_cover_urls)
        return [(res if res.status_code == 200 else None) for res in grequests.map(rs)]

    def parse_ps1serial(
        self,
        region: str,
        letter: str,
        serial: str,
        response,
        href_data: str,
    ):
        if not region or not letter or not serial:
            return
        hires_results = self.check_hires(
            [
                f"https://psxdatacenter.com/images/hires/{region}/{letter}/{serial}/{serial}-F-ALL.jpg",
                f"https://psxdatacenter.com/images/covers/{region}/{letter}/{serial}/{serial}-F-ALL.jpg",
            ]
        )

        hires_request = next(
            (res for res in hires_results if res is not None), response
        )
        image_url = (
            hires_request.url
            if "-F-ALL.jpg" in hires_request.url
            else response.urljoin(href_data)
        )
        image_body = (
            hires_request.content
            if hasattr(hires_request, "content")
            else hires_request.body
        )
        checksum = md5sum(BytesIO(image_body))
        local_cover_file = f"{COVERS_PATH}/{serial}.jpg"
        need_to_download = False
        if os.path.exists(local_cover_file):
            local_checksum = ""
            with open(local_cover_file, "rb") as f:
                local_checksum = hashlib.file_digest(f, "md5").hexdigest()
            if checksum != local_checksum:
                need_to_download = True
        else:
            need_to_download = True
        if need_to_download:
            return CoverImageItem(
                serial=serial,
                image_urls=[image_url],
                checksum=checksum,
            )

    def parse(self, response):
        if f"{self.start_urls[-1]}" in response.url:
            for href in response.xpath("/html/body/pre/a/@href"):
                href_data = href.get()
                # follow the directory links
                if href_data[-1] == "/" and (
                    not response.urljoin(href_data) == response.url
                    or response.urljoin(href_data) == self.start_urls[-1]
                ):
                    yield response.follow(href_data, self.parse)
                # get only the FRONT cover images
                region = ""
                letter = ""
                serial = ""
                try:
                    region, letter, serial = re.findall(
                        r"([A-Z]{1})\/([A-Z0-9\-]{1,3})\/([A-Z]{3,4}-[0-9]{1,5})\.jpg",
                        response.urljoin(href_data),
                    )[0]
                except:
                    yield
                yield self.parse_ps1serial(region, letter, serial, response, href_data)


process = CrawlerProcess(
    settings={
        "FEEDS": {
            "last_run.json": {
                "format": "json",
                "fields": ["serial", "checksum"],
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
