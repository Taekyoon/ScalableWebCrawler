import pytest

from Crawler import SingleHTMLDownloader

# TODO: Add Has HTML Format source test - should be tested by negative method
# TODO: Add Set 'UTF-8' test

@pytest.fixture
def googleSiteDownloader():
    return SingleHTMLDownloader('http://google.com')

@pytest.fixture
def sampleBBCArticleSiteDownloader():
    return SingleHTMLDownloader('http://www.bbc.com/sport/football/37067781')

@pytest.fixture
def sampleYelpBlogSiteDownloader():
    return SingleHTMLDownloader('http://engineeringblog.yelp.com/' +
                                '2016/04/distributed-tracing-at-yelp.html')

def test_request_Google_status(googleSiteDownloader):
    "This request should always be 'OK'"
    googleSiteDownloader.request_source()
    assert googleSiteDownloader.get_downloaded_resource_status() == 200

def test_request_BBCArticle_status(sampleBBCArticleSiteDownloader):
    "This request should always be 'OK'"
    sampleBBCArticleSiteDownloader.request_source()
    assert sampleBBCArticleSiteDownloader.get_downloaded_resource_status() == 200

def test_request_YelpBlog_status(sampleYelpBlogSiteDownloader):
    "This request should always be 'OK'"
    sampleYelpBlogSiteDownloader.request_source()
    assert sampleYelpBlogSiteDownloader.get_downloaded_resource_status() == 200

def test_validate_unrequest(googleSiteDownloader):
    with pytest.raises(ValueError) as excinfo:
        googleSiteDownloader.validate_response_variable()
        assert excinfo.value.message == 'This Object could not be requested.'
