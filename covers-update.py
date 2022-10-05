import re

import scrapy
from scrapy import Field, Item, Request
from scrapy.crawler import CrawlerProcess
from scrapy.pipelines.images import ImagesPipeline


class CoverImagesPipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        return [
            Request(x, meta={"serial": item["serial"]})
            for x in item.get("image_urls", [])
        ]

    def file_path(self, request, response=None, info=None, *, item=None):
        return "%s.jpg" % request.meta["serial"]


class CoverImageItem(Item):
    serial = Field()
    image_urls = Field()
    images = Field()


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
                        r"([A-Z]{3,4}-[0-9]{1,5})-F-ALL\.jpg|([A-Z]{3,4}-[0-9]{1,5})\.jpg", href_data
                    )[0]
                    ps1serial = format1 if format1 else format2
                except:
                    yield
                if ps1serial:
                    yield CoverImageItem(
                        serial=ps1serial, image_urls=[response.urljoin(href_data)]
                    )


process = CrawlerProcess(
    settings={
        "FEEDS": {
            "available_covers.csv": {
                "format": "csv",
                "fields": ["serial"],
                "item_classes": [CoverImageItem],
                "overwrite": True,
            },
        },
        "IMAGES_STORE": "covers",
        "DOWNLOAD_DELAY": 0.2,
        "ITEM_PIPELINES": {CoverImagesPipeline: 1},
    }
)

process.crawl(PsxDataCenterCoverSpider)
process.start()
