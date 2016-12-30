import json

class SingleRecordConstructor:

    def __init__(self, resource):
        self.resource_id = resource.get_ID()
        self.resource_org_place = resource.get_org_place()
        self.resource_org_place_len = resource.get_org_place_len()
        self.resource_text = resource.get_text()
        self.resource_text_len = resource.get_text_len()
        self.resource_download_time = resource.get_download_time()

    def get_constructed_Value(self, Type = 'json'):
        if(Type == 'json'):
            return json.dumps({'org' : self.resource_org_place,
                               'org_len' : self.resource_org_place_len,
                               'text' : self.resource_text.decode('utf-8'),
                               'text_len' : self.resource_text_len,
                               'downloaded_time' : self.resource_download_time}
                              )

    def get_resource_id(self):
        return self.resource_id
