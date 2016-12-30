from Crawler.single_html_resource import SingleHTMLResource as resource
from Crawler.single_record_constructor import SingleRecordConstructor as constructor
from Crawler.resource_storer import ResourceStorer as storer

class SingleHTMLCrawler:
    '''
    This is a class for using test crawl one time.
    But it needs to make more professional..
    So later this class should be changed more fascinating.
    '''

    def __init__(self, url):
        self.url = url
        self.storer = storer('./test')
        self.record = constructor(resource(self.url))

    def crawl(self):
        self.storer.put(self.record.get_resource_id(), self.record.get_constructed_Value())

    def test_find(self):
        r = self.storer.get(self.record.get_resource_id())
        if(r.value.decode('utf-8') == self.record.get_constructed_Value()):
            print('Success!!')


test_crawler = SingleHTMLCrawler('http://www.bbc.com/news/technology-37192670')
test_crawler.crawl()
test_crawler.test_find()