import concurrent.futures
import json
from datetime import datetime
import re
import logging



import scrapy


class KnowYourMeme(scrapy.Spider):
    name = 'KnowYourMeme'
    allowed_domains = ['knowyourmeme.com']
    base_url = 'https://knowyourmeme.com'
    start_urls = ['https://knowyourmeme.com/memes/all?sort=oldest']

    relation_cache = {}

    def parse_entry(self, response: scrapy.http.Response):

        header = response.xpath('//article[@class="entry"]/header')
        image = header.xpath('div[@class="photo-wrapper"]/a[@class="full-image"]/@href').get()
        info = header.xpath('section[contains(@class, "info")]')
        *_, added, updated = [None] + [datetime.fromisoformat(ts.get()).timestamp()
                                       for ts in info.xpath('.//abbr[@class="timeago"]/@title')]

        title = info.xpath('h1/text()').get().strip()
        category = info.xpath('.//a[@class="category"]/span/text()').get()
        meta = {e.xpath('./@property').get() or e.xpath('./@name').get(): e.xpath('./@content').get()
                for e in response.xpath(
                '//head/meta[(@name and @name!="viewport" and @name!="referrer" and @name!="p:domain_verify") or (@property)]')}

        entry = {
            'title': title,
            'url': response.url,
            'last_update_source': int(updated),
            'category': category,
            'template_image_url': image,
            'meta': meta
        }

        ld = response.xpath('//*[@id="maru"]/script/text()').get()
        if ld:
            entry['ld'] = json.loads(ld)

        if added:
            entry['added'] = int(added)

        parent = info.xpath('.//span[contains(text(), "Part of a series on")]/a/@href').get()
        if parent:
            entry['parent'] = self.base_url + parent
            yield response.follow(parent, callback=self.parse_entry)

        siblings = info.xpath('.//p[@class="parent"]/a/@href').get()
        children = info.xpath('.//p[@class="children"]/a/@href').get()

        entry['details'] = {}
        for div in info.xpath('div[@class="details"]//div[@class="detail"]'):
            detail, values = div.xpath('.//span')
            detail = detail.xpath('text()').get()[:-1].lower()
            if detail == 'year':
                values = values.xpath('a/text()').get()
            elif detail == 'type':
                values = [self.base_url + path.get() for path in values.xpath('a/@href')]
            else:
                values = values.xpath('text()').get().strip() or values.xpath('a/@href')
            entry['details'][detail] = values

        if category == "Meme":
            body_ref = header.xpath('following-sibling::div[@id="entry_body"]')
            primary_headings = {'h1', 'h2', 'h3'}
            secondary_headings = {'h4', 'h5', 'h6'}
            body = {}
            section = {}
            body_section = {}
            for s in body_ref.xpath('section[@class="bodycopy"]/div[@class="entry-section-container"]'
                                    '//*[substring-after(name(), "h") > 0 or self::p or self::img]'):
                tag = s.xpath('name()').get()
                if tag == 'img':
                    section.setdefault('images', []).append(s.xpath('@data-src').get())
                    continue
                text = s.xpath('normalize-space()').get()
                if tag == 'p':
                    if text:
                        section.setdefault('text', []).append(text)
                    for link in s.xpath('.//a'):
                        url = link.xpath('@href').get() or link.xpath('@hrf').get()
                        if not url or url.startswith('#'):
                            continue
                        classes = link.xpath('@class').get()
                        if classes and 'external-link' not in classes:
                            url = self.base_url + url
                        section.setdefault('links', []).append((link.xpath('string()').get(), url))
                if tag in primary_headings:
                    body_section = section = {}
                    body[text.lower()] = section
                if tag in secondary_headings:
                    section = {}
                    body_section.setdefault('subsections', {})[text.lower()] = section
            tags = body_ref.xpath('following-sibling::div//div[@class="tags"]/a/text()').getall()
            additional_references = {tag.xpath('text()').get(): tag.xpath('@href').get()
                                     for tag in body_ref.xpath('following-sibling::div//div[@class="other-links"]/a')}
            entry.update(
                content=body,
                tags=tags,
                additional_references=additional_references
            )
        search_interest = header.xpath(
            'following-sibling::div[@id="entry_body"]//div[@class="google-trends-embed-wrapper"]/script[2]/text()').get()
        if search_interest:
            keywords = re.findall("\"keyword\":\"(.+?)\",\"geo", search_interest)
            entry['search_keywords'] = keywords

        entry_done = True
        if children:
            child_list = []
            entry['children'] = child_list
            entry['children_done'] = False
            if children in self.relation_cache and self.relation_cache[children]['done']:
                logging.debug(f"existing relations from: {children}")
                child_list.extend(self.relation_cache[children])
            else:
                entry_done = False
                yield response.follow(children, self.parse_relation, cb_kwargs={'future': child_future, 'key': children},
                                      errback=lambda _: child_future.set_result(False))
        if siblings:
            sibling_list = []
            entry['siblings'] = sibling_list
            entry['siblings_done'] = False
            if siblings in self.relation_cache and self.relation_cache[siblings]['done']:
                logging.debug(f"existing relations from: {siblings}")
                sibling_list.extend(self.relation_cache[siblings])
            else:
                entry_done = False
                yield response.follow(siblings, self.parse_relation, cb_kwargs={'future': sibling_future, 'key': siblings},
                                      errback=lambda _: sibling_future.set_result(False))
        if futures:
            logging.debug("awaiting futs")
            concurrent.futures.wait(futures)
            logging.debug("got futs")
            if children:
                logging.debug(f"got relations for: {children}")
                entry['children'] = self.relation_cache[children]
            if siblings:
                logging.debug(f"got relations for: {siblings}")
                entry['siblings'] = self.relation_cache[siblings]
        if entry_done:
            yield entry

    def parse_relation(self, response: scrapy.http.Response, future, key):
        logging.debug(f"parsing relations: {key}")
        self.relation_cache.setdefault(key, set()).update(self.base_url + path.get() for path in
                                              response.xpath('//table[@class="entry_list"]//td/h2/a/@href'))
        next_page = response.xpath('//a[@rel="next"]/@href').get()
        if next_page:
            yield response.follow(next_page, self.parse_relation, cb_kwargs=response.cb_kwargs,
                                  errback=lambda _: future.set_result(False))
        else:
            logging.debug(f"relations done: {key}")
            future.set_result(True)

    def parse(self, response: scrapy.http.Response, **_):
        yield from (scrapy.Request(self.base_url + path.get(), callback=self.parse_entry)
                    for path in response.xpath('//table[@class="entry_list"]//td/h2/a/@href'))
        next_page = response.xpath('//a[@rel="next"]/@href').get()
        if next_page:
            yield response.follow(next_page)


if __name__ == '__main__':
    from scrapy.utils.log import configure_logging
    from scrapy.crawler import CrawlerRunner
    from twisted.internet import reactor
    from scrapy.utils.project import get_project_settings

    configure_logging()
    runner = CrawlerRunner(get_project_settings())
    runner.crawl(KnowYourMeme).addBoth(lambda _: reactor.stop())
    reactor.run()
