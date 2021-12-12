import requests


class HttpWrapper(object):

    @staticmethod
    def get(url):
        return requests.get(
            url,)

    @staticmethod
    def post(url, data, headers={
        'Content-type': 'application/json',
    }):
        return requests.post(
            url, headers=headers, json=data)
