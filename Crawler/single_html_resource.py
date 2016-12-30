from Crawler.single_html_downloader import SingleHTMLDownloader as downloader

class SingleHTMLResource:

    def __init__(self, url, compress_ratio = 1):
        getter = downloader(url)
        getter.request_source()

        self.ID = self.set_ID(self.set_netloc_and_path(url))
        self.org_place = getter.get_downloaded_resource_url()
        self.org_place_len = getter.get_downloaded_resource_url_length()
        if(compress_ratio > 0):
            self.text = getter.get_compressed_text(compress_ratio)
            self.text_len = getter.get_compressed_text_length(compress_ratio)
        else:
            self.text = getter.get_downloaded_text()
            self.text_len = getter.get_downloaded_text_length()
        self.download_time = getter.get_downloaded_time()

    def set_ID(self, src):
        import hashlib

        checksum = hashlib.md5()
        checksum.update(src)

        return checksum.hexdigest().encode('utf-8')

    def set_netloc_and_path(self, url):
        from urllib.parse import urlparse

        ret_obj = urlparse(url)
        return (ret_obj.netloc + ret_obj.path).encode('utf-8')


    def get_ID(self): return self.ID

    def get_org_place(self): return self.org_place

    def get_org_place_len(self): return self.org_place_len

    def get_text(self): return self.text

    def get_text_len(self): return self.text_len

    def get_download_time(self): return self.download_time
