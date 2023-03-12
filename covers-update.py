import re
import os
import hashlib

import scrapy
from scrapy import Field, Item, Request
from scrapy.crawler import CrawlerProcess
from scrapy.pipelines.images import ImagesPipeline
from io import BytesIO
from scrapy.utils.misc import md5sum

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
        "https://psxdatacenter.com/images/",
    ]

    def parse(self, response):
        if (
            response.url == self.start_urls[-1]
            or f"{self.start_urls[-1]}covers/" in response.url
            or f"{self.start_urls[-1]}hires/" in response.url
        ):
            for href in response.xpath("/html/body/pre/a/@href"):
                href_data = href.get()
                # follow the directory links
                if href_data[-1] == "/":
                    yield response.follow(href_data, self.parse)
                # get only the FRONT cover images
                ps1serial = None
                try:
                    format1, format2 = re.findall(
                        r"([A-Z]{3,4}-[0-9]{1,5})-F-ALL\.jpg|([A-Z]{3,4}-[0-9]{1,5})\.jpg",
                        href_data,
                    )[0]
                    ps1serial = format1 if format1 else format2
                except:
                    yield
                if ps1serial:
                    checksum = md5sum(BytesIO(response.body))
                    local_cover_file = f"{COVERS_PATH}/{ps1serial}.jpg"
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
                        yield CoverImageItem(
                            serial=ps1serial,
                            image_urls=[response.urljoin(href_data)],
                            checksum=checksum,
                        )
                yield


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
