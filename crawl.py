from datetime import date, datetime
from env import *
from requests_ip_rotator import ApiGateway
import orjson, random, asyncio, aiohttp
import ipaddress
from bs4 import BeautifulSoup

MAX_IPV4 = ipaddress.IPv4Address._ALL_ONES


def getEndpoints():
  gateway = ApiGateway(gatewayUrl, regions=gatewayRegions, access_key_id=gatewayKey['id'], access_key_secret=gatewayKey['password'])
  gateway.start()
  endpoints = gateway.endpoints
  return endpoints


def appendBenchmark(keyword: str, gender: int, age: int, device: int):
  if gender != 0: return [keyword, benchmarkKeyword['gender']]
  if age != 0: return [keyword, benchmarkKeyword['age']]
  if device != 0: return [keyword, benchmarkKeyword['device']]
  return [keyword, benchmarkKeyword['default']]


def getKeywordStats(keyword: str, startDate: date, endDate: date, endpoints: list):
  keywordStats = []
  asyncio.get_event_loop().run_until_complete(getKeywordStatsAsync(keyword, startDate, endDate, endpoints, keywordStats))
  return keywordStats


def getQueryGroups(keywordInput: list):
  queryGroups = [ keyword + '__SZLIG__' + keyword for keyword in keywordInput ]
  queryGroups = '__OUML__'.join(queryGroups)
  return queryGroups


def getRandomHash(length: int):
  hash = [random.choice('0123456789abcdef') for i in range(length)]
  return ''.join(hash)


def getQueryDate(startDate: date, endDate: date):
  dateFormat = '%Y%m%d'
  numStartDate = int(datetime.strftime(startDate, dateFormat))
  numEndDate = int(datetime.strftime(endDate, dateFormat))
  return numStartDate, numEndDate


async def getHashKey(keywordInput: list, gender: int, age: int, device: int, startDate: date, endDate: date, endpoints: list):
  endpoint = random.choice(endpoints)
  forwarded = ipaddress.IPv4Address._string_from_ip_int(random.randint(0, MAX_IPV4))
  url = 'https://datalab.naver.com/qcHash.naver'
  queryGroups = getQueryGroups(keywordInput)
  queryStartDate, queryEndDate = getQueryDate(startDate, endDate)
  params = {
    'queryGroups': queryGroups,
    'startDate': queryStartDate,
    'endDate': queryEndDate,
    'timeUnit': 'date',
    'gender': query['gender'][gender],
    'age': query['age'][age],
    'device': query['device'][device]
  }
  refererHash = getRandomHash(32)
  headers = {
    'origin': 'https://datalab.naver.com',
    'referer': 'https://datalab.naver.com/keyword/trendResult.naver?hashKey=' + refererHash,
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Host': endpoint,
    'X-My-X-Forwarded-For': forwarded,
  }
  _, site = url.split('//', 1)
  sitePath = site.split('/', 1)[1]
  proxyUrl = 'https://' + endpoint + '/ProxyStage/' + sitePath
  async with aiohttp.ClientSession() as session:
    async with session.post(url = proxyUrl, params = params, headers = headers) as req:
      data = await req.json(content_type = 'text/html')
  
  if data['success'] is False: raise ValueError
  return data['hashKey']


async def getKeywordStatsAsync(keyword: str, startDate: date, endDate: date, endpoints: list, keywordStats: list):
  filteredStats = await asyncio.gather(*[getFilteredStatsAsync(keyword, filter[0], filter[1], filter[2], startDate, endDate, endpoints) for filter in filters])
  keywordStats.extend(filteredStats)
  return


async def getFilteredStatsAsync(keyword: str, gender: int, age: int, device: int, startDate: date, endDate: date, endpoints: list):
  endpoint = random.choice(endpoints)
  forwarded = ipaddress.IPv4Address._string_from_ip_int(random.randint(0, MAX_IPV4))
  keywordInput = appendBenchmark(keyword, gender, age, device)
  hashKey = await getHashKey(keywordInput, gender, age, device, startDate, endDate, endpoints)
  url = 'https://datalab.naver.com/keyword/trendResult.naver?hashKey=' + hashKey
  headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Host': endpoint,
    'X-My-X-Forwarded-For': forwarded
  }
  _, site = url.split('//', 1)
  sitePath = site.split('/', 1)[1]
  proxyUrl = 'https://' + endpoint + '/ProxyStage/' + sitePath
  async with aiohttp.ClientSession() as session:
    async with session.get(url = proxyUrl, headers = headers) as req:
      txt = await req.text()
  soup = BeautifulSoup(txt, 'lxml')
  stats = orjson.loads(soup.find('div', id='graph_data').get_text())
  return stats


if __name__ == '__main__':
  keyword = 'keyword'
  startDate = date(2022, 1, 1)
  endDate = date(2022, 4, 30)
  endpoints = getEndpoints()
  keywordStats = getKeywordStats(keyword, startDate, endDate, endpoints)
  print(keywordStats)
