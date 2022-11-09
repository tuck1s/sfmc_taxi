#!/usr/bin/env bash
import requests, redis, os, json
from urllib.parse import urljoin, urlencode
# debug import curlify

# Persistent token, uses Redis for storage
class PersistentAuthToken:
    def __init__(self, et_subdomain:str, et_clientID:str, et_clientSecret:str):
        redisUrl = os.getenv('REDIS_URL', default='redis://localhost:6379')
        self.r = redis.from_url(redisUrl, socket_timeout=5)
        self.tokPrefix = 'sfmc_taxi:'
        self.tokName = 'access_token'
        # Collect auth params
        if et_subdomain:
            self.et_url = urljoin('https://' + et_subdomain + '.auth.marketingcloudapis.com', '/v2/token')
        else:
            raise ValueError('Parameter et_subdomain not defined')
        if et_clientID:
            self.et_clientID = et_clientID
        else:
            raise ValueError('Parameter et_clientID not defined')
        if et_clientSecret:
            self.et_clientSecret = et_clientSecret
        else:
            raise ValueError('Parameter et_clientSecret not defined')

    def get(self):
        access_token = self.r.get(self.tokName)
        if access_token:
            return access_token
        else:
            # Get a fresh token. See https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/access-token-s2s.html
            res = requests.post(self.et_url, headers = {'Content-Type' : 'application/json'},
            json = {
                'grant_type': 'client_credentials',
                'client_id': self.et_clientID,
                'client_secret': self.et_clientSecret,
            })
            if res.status_code != 200:
                raise ValueError('Authentication error', res.status_code, res.text)
            else:
                    r = res.json()
                    access_token = r.get(self.tokName)
                    expires_in = r.get('expires_in')
                    scope = r.get('scope')
                    if access_token and expires_in and scope:
                        # safely stop using it 2 mins before it expires
                        self.set(access_token, max(0, expires_in-120), scope)
                        return access_token
                    else:
                        raise ValueError('Invalid access_token, expires_in, scope returned', access_token, expires_in, scope)

    def set(self, access_token:str, ttl:int, scope:str):
        self.r.set(self.tokPrefix + self.tokName, access_token, ex=ttl)
        self.r.set(self.tokPrefix + 'scope', scope, ex=ttl)


if __name__ == '__main__':
    et_subdomain = os.getenv('et_subdomain')
    tok = PersistentAuthToken(et_subdomain, os.getenv('et_clientID'), os.getenv('et_clientSecret'))
    auth = tok.get()
    # Fetch an inventory of images
    asset = []
    page = 1
    while(True):
        # See https://developer.salesforce.com/docs/marketing/marketing-cloud/guide/assetSimpleQuery.html
        list_assets_url = urljoin('https://' + et_subdomain + '.rest.marketingcloudapis.com', '/asset/v1/content/assets')

        # NB SFMC does not accept % encoding on the $filter setting name, so have to force custom encoding
        # It also expects comma and = to be passed through
        # NB filter needs to match on name "image" (lowercase) not "Image"
        p = urlencode( {
            '$page': page,
            '$pagesize': 400,
            '$orderBy': 'id asc',
            '$filter': 'assetType.displayName=image',
            '$fields': 'id,customerKey,fileProperties,assetType'
        }, safe='/$=,')
        res = requests.get(list_assets_url, params=p, headers= {
            'Content-Type' : 'application/json',
            'Authorization': 'Bearer {}'.format(auth),
        })
        if res.status_code != 200:
            raise ValueError(res.status_code, res.text)
        else:
            # debug print(curlify.to_curl(res.request))
            resObj = res.json()
            if resObj.get('count') > 0:
                for i in resObj.get('items'):
                    asset.append(i)
                page +=1
            else:
                break
    print(json.dumps(asset, indent=2))

