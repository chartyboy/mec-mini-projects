import sqlite3
import json
import scrapy
import os
from scrapy.crawler import CrawlerProcess


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


rel_path = r"./scrapy_mini_project/scrapy_mini_project/xpath-scraper-results.json"
if not os.path.exists(rel_path):
    # Run spider
    setting = {"FEEDS": {rel_path: {"format": "json"}}}
    process = CrawlerProcess(settings=setting)
    process.crawl(QuotesSpider)
    process.start()

with open(rel_path, encoding="utf-8") as f:
    data = json.load(f, encoding="utf-8")

# Insert records into database
if os.path.exists(r"./quote.db"):
    os.remove(r"./quote.db")

con = sqlite3.connect(r"./quote.db")
con.row_factory = sqlite3.Row
cur = con.cursor()
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS authors(
        auth_id INTEGER PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        birthday DATE,
        birthplace VARCHAR(255),
        description TEXT(65535)
    )
    """
)
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS quotes(
        id INTEGER PRIMARY KEY,
        author_id INTEGER,
        quote TEXT(255) NOT NULL,
        FOREIGN KEY (author_id) REFERENCES authors(auth_id)
    );
    """
)
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS tags(
        tag_id INTEGER PRIMARY KEY,
        tag VARCHAR(255) NOT NULL
    )
    """
)
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS tagged_quotes(
        tag_q_id INTEGER PRIMARY KEY,
        tag_id INTEGER,
        quote_id INTEGER,
        FOREIGN KEY(tag_id) REFERENCES tags(tag_id),
        FOREIGN KEY (quote_id) REFERENCES quotes(id)
    )
    """
)


# Insert data in json file into db
quotes = list()
tags = set()
authors = list()
for entry in data:
    scraped = entry["data"]
    if entry["type"] == "quote":
        quotes.append((scraped["author"], scraped["text"], scraped["tags"]))
        tags = tags.union(set(scraped["tags"]))
    elif entry["type"] == "author":
        authors.append(
            (
                scraped["author_name"],
                scraped["birthday"],
                scraped["country"],
                scraped["desc"],
            )
        )
tags = sorted(list(tags))
tag_map = {tag: id for id, tag in enumerate(tags)}

cur.executemany(
    """
    INSERT INTO authors (name, birthday, birthplace, description) VALUES (?,?,?,?)
    """,
    authors,
)
con.commit()
authors = cur.execute("SELECT auth_id, name FROM authors")
auth_map = {author["name"]: author["auth_id"] for author in authors}
quote_to_tags = list()


def create_tagged_rows(quote_id, tag_names, tag_map):
    return [(tag_map[name], quote_id) for name in tag_names]


for i, quote in enumerate(quotes):
    auth_name = quote[0]
    if auth_name in auth_map.keys():
        quotes[i] = (i, auth_map[quote[0]], quote[1].strip('"'))
    else:
        quotes[i] = (i, -1, quote[1].strip('"'))
        pass
    quote_to_tags.extend(create_tagged_rows(i, quote[2], tag_map))


cur.executemany(
    """
    INSERT INTO quotes (id, author_id, quote) VALUES (?, ?, ?)
""",
    quotes,
)
cur.executemany(
    """
    INSERT INTO tags (tag, tag_id) VALUES (?, ?)
    """,
    tag_map.items(),
)
cur.executemany(
    """
    INSERT INTO tagged_quotes (tag_id, quote_id) VALUES (?, ?)
    """,
    quote_to_tags,
)
con.commit()

# Select all quotes tagged "inspirational"
res = cur.execute(
    """
    SELECT q.quote
    FROM quotes as q, tagged_quotes as tq, tags as t  
    WHERE tq.tag_id = t.tag_id
    AND t.tag = 'inspirational'
    AND q.id = tq.quote_id
    """
)
print(res.fetchone()[0])
