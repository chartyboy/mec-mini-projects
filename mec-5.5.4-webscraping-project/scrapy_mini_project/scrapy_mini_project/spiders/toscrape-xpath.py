import scrapy


class QuotesSpider(scrapy.Spider):
    name = "toscrape-xpath"

    def start_requests(self):
        url = "http://quotes.toscrape.com/"
        tag = getattr(self, "tag", None)
        if tag is not None:
            url = url + "tag/" + tag
        yield scrapy.Request(url, self.parse)

    def parse(self, response):
        for quote in response.xpath("//div[@class='quote']"):
            yield {
                "type": "quote",
                "data": {
                    "text": quote.xpath(".//span[@class='text']/text()").get(),
                    "author": quote.xpath(".//small[@class='author']/text()").get(),
                    "tags": [
                        tag.get()
                        for tag in quote.xpath(
                            ".//div[@class='tags']/a[@class='tag']/text()"
                        )
                    ],
                },
            }
            auth_page = quote.xpath("./span/a/@href").get()
            if auth_page is not None:
                auth_page = response.urljoin(auth_page)
                yield response.follow(auth_page, self.parse_author)

        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield response.follow(next_page, self.parse)

    def parse_author(self, response):
        # Get name, birthday, country of origin
        info = response.xpath("//div[@class='author-details']")
        location = info.xpath(".//span[@class='author-born-location']").get()
        yield {
            "type": "author",
            "data": {
                "author_name": info.xpath(".//h3/text()").get(),
                "birthday": info.xpath(
                    ".//span[@class='author-born-date']/text()"
                ).get(),
                "country": location.split(" ")[-1],
                "desc": info.xpath(".//div[@class='author-description']/text()")
                .get()
                .strip(" \n"),
            },
        }
