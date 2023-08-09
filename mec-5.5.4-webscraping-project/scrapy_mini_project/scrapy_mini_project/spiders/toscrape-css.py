import scrapy


class QuotesSpider(scrapy.Spider):
    name = "toscrape-css"

    def start_requests(self):
        url = "http://quotes.toscrape.com/"
        tag = getattr(self, "tag", None)
        if tag is not None:
            url = url + "tag/" + tag
        yield scrapy.Request(url, self.parse)

    def parse(self, response):
        for quote in response.css("div.quote"):
            yield {
                "type": "quote",
                "data": {
                    "text": quote.css("span.text::text").get(),
                    "author": quote.css("small.author::text").get(),
                    "tags": quote.css("div.tags").css("a.tag::text").getall(),
                },
            }
            auth_page = quote.css("span").css("a::attr(href)").get()
            if auth_page is not None:
                auth_page = response.urljoin(auth_page)
                yield response.follow(auth_page, self.parse_author)

            next_page = response.css("li.next a::attr(href)").get()
            if next_page is not None:
                next_page = response.urljoin(next_page)
                yield scrapy.Request(next_page, callback=self.parse)

    def parse_author(self, response):
        # Get name, birthday, country of origin
        info = response.css("div.author-details")
        location = info.css("span.author-born-location::text").get()
        yield {
            "type": "author",
            "data": {
                "author_name": info.css("h3::text").get(),
                "birthday": info.css("span.author-born-date::text").get(),
                "country": location.split(", ")[-1],
                "desc": info.css("div.author-description::text").get().strip(" \n"),
            },
        }
