import requests

import zlib, base64

__all__ = ('SingleHTMLDownloader')


class SingleHTMLDownloader:

    def __init__(self, requested_url):
        self.response = None
        self.requested_url = requested_url
        self.is_html = False
        self.is_response_OK = True

    def request_source(self):
        # TODO: Still need to check 'is the Exception work?'
        import re
        try:
            self.response = requests.get(self.requested_url)
        except ConnectionError:
            raise ConnectionError(
                'Request to ' + self.requested_url + ' could be ' +
                'failed.')

        if(self.response.status_code != 200):
            self.is_response_OK = False
            self.is_html = False
            pass

        if(self.response.headers['Content-Type'].split(';')[0]
           == 'text/html'):
            print(self.response.headers['Content-Type'].split(';'))
            #html_verifier = re.compile(
            #    '<!([Dd][Oo][Cc][Tt][Yy][Pp][Ee])( +)([Hh][Tt][Mm][Ll])')
            #if(html_verifier.search(self.response.text) is not None):
            self.is_html = True

    def validate_response_variable(self):
        if (self.response is None):
            raise ValueError('This Object could not be requested.')
        if(not self.is_html):
            raise AttributeError('This Object does not have HTML Source')
        if(not self.is_response_OK):
            raise AttributeError('This Object does not have http status 200')

    def get_downloaded_text(self):
        # All response texts are 'UTF-8' encoded character.
        self.validate_response_variable()

        if(self.is_html and self.is_response_OK):
            return self.response.text.encoding('UTF-8')

    def get_compressed_text(self, compressed_ratio):
        self.validate_response_variable()

        return base64.b64encode(zlib.compress(
                self.response.text.encode('UTF-8'),
                compressed_ratio))

    def get_downloaded_resource_status(self):
        self.validate_response_variable()

        return self.response.status_code

    def get_downloaded_resource_url(self):
        self.validate_response_variable()

        return self.response.url

    def get_downloaded_resource_url_length(self):
        self.validate_response_variable()

        return len(self.response.url)

    def get_downloaded_text_length(self):
        self.validate_response_variable()

        return len(self.response.text)

    def get_compressed_text_length(self, compressed_ratio):
        self.validate_response_variable()

        return len(base64.b64encode(zlib.compress(
            self.response.text.encode('UTF-8'),
            compressed_ratio)))

    def get_downloaded_time(self):
        self.validate_response_variable()

        return self.response.headers['Date']
