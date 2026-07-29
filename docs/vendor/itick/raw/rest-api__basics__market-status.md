<!-- source: https://docs.itick.org/zh-cn/rest-api/basics/market-status -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: html | slug: rest-api/basics/market-status -->

# 免费金融市场API 金融市场假期与市场状态API 市场状态 交易时间 金融行情数据API REST API 查询指定国家或地区的市场当前状态，返回是否开市、状态、交易时段、时区和当地时间。

1. [文档](/zh-cn)
2. [REST API](/zh-cn/rest-api/basics/symbol-list)
3. [基础数据](/zh-cn/rest-api/basics/symbol-list)
4. [市场状态](/zh-cn/rest-api/basics/market-status)

Copy

API KEY

请选择API KEY

* 立即登录/注册

## 市场状态查询

GET

/market/status

### 请求参数

codeenum必填

香港

* 中国(大陆)
* 香港
* 美国
* 新加坡
* 台湾
* 日本
* 印度
* 泰国
* 德国
* 墨西哥
* 马来西亚
* 土耳其
* 西班牙
* 荷兰
* 英国
* 印尼
* 越南
* 意大利
* 法国
* 澳大利亚
* 阿根廷
* 以色列
* 巴基斯坦
* 加拿大
* 秘魯
* 尼日利亚
* 肯尼亚
* 罗马尼亚
* 瑞士
* 摩洛哥

国家/地区代码

### 响应参数

codenumber

响应code

msgstring

响应描述

dataarray(object)

响应结果

cstring

国家/地区代码

rstring

国家/地区名称

ostring

是否开市

sstring

市场状态；`PRE\_MARKET` 盘前，`OPEN` 盘中，`LUNCH\_BREAK` 盘中休盘（如午休），`POST\_MARKET` 盘后，`CLOSED` 非交易时段，`CLOSED\_WEEKEND` 周末休市，`CLOSED\_HOLIDAY` 假期休市，`HOLIDAY\_HALF\_DAY` 假期半日市开市中

hstring

当天假期名称，仅在 `status = CLOSED\_HOLIDAY / HOLIDAY\_HALF\_DAY` 时有值

tstring

交易时段配置

zstring

时区编码

lstring

当前地区本地时间，格式 `yyyy-MM-dd HH:mm:ss`

### 代码示例

```
import requests

url = "https://api.itick.org/market/status?code=HK"

headers = {
"accept": "application/json"
"token": "Your Token"
}

response = requests.get(url, headers=headers)

print(response.text)
```

### 查询 URL

GET

https://api.itick.org/market/status?code=HK

### 响应结果

响应示例查询结果

```
{
  "code": 0,
  "msg": "ok",
  "data": {
    "c": "es",
    "r": "Spain",
    "o": false,
    "s": "CLOSED_WEEKEND",
    "h": null,
    "t": "08:00-09:00,09:00-17:30,",
    "z": "Europe/Madrid",
    "l": "2026-07-05 18:47:44"
  }
}
```

1. [市场假期

   提供准确、及时的全球金融市场假期数据API接口，覆盖A股、港股、美股等主要交易所。数据来源可靠、更新及时，通过RESTful接口轻松集成，免费试用。](/zh-cn/rest-api/basics/symbol-holidays)
2. [加密货币实时成交

   覆盖比特币(BTC)、以太坊(ETH)等数千种代币的最新价格、买一卖一、24小时成交量等深度字段。低延迟、高频率，为您的量化交易、行情看板与DApp提供核心数据支持。免费试用。](/zh-cn/rest-api/crypto/crypto-tick)
