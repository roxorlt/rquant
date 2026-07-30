<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-split -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: html | slug: rest-api/stocks/stock-split -->

# 股票实时行情API 除权因子API接口 全球股票复权数据接口 复权因子接口 免费股票API接口 REST API 提供精准复权，助力量化。提供A股、港股、美股等全球市场完整除权除息数据，覆盖分红、送股、配股等事件。数据准确、更新及时，通过高效API接口或批量文件，轻松获取复权因子、复权价格，是量化分析、策略回测及金融应用的可靠数据基石。

1. [文档](/zh-cn)
2. [REST API](/zh-cn/rest-api/basics/symbol-list)
3. [股票数据](/zh-cn/rest-api/stocks/stock-tick)
4. [除权因子](/zh-cn/rest-api/stocks/stock-split)

Copy

API KEY

请选择API KEY

* 立即登录/注册

## 股票除权因子

GET

/stock/split

### 请求参数

regionenum必填

港股

* 中国(大陆)
* 港股
* 美股
* 上证
* 深证
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

市场代码，支持HK（港股）、SH（上证）、SZ（深证）、US（美股）、SG（新加坡）、JP（日本）、TW（中国台湾）、IN（印度）、TH（泰国）、DE（德国）、MX（墨西哥）、MY（马来西亚）、TR（土耳其）、ES（西班牙）、NL（荷兰）、GB（英国）等

### 响应参数

codenumber

响应code

msgstring

响应描述

dataobject

响应结果

dnumber

复权日期时间戳（单位：毫秒）

rstring

国家/地区代码

nstring

股票名称

cstring

股票代码

vstring

复权因子，拆股/合股的比例

### 代码示例

```
import requests

url = "https://api.itick.org/stock/split?region=HK"

headers = {
"accept": "application/json"
"token": "Your Token"
}

response = requests.get(url, headers=headers)

print(response.text)
```

### 查询 URL

GET

https://api.itick.org/stock/split?region=HK

### 响应结果

响应示例查询结果

```
{
  "code": 0,
  "msg": "ok",
  "data": {
    "content": [
      {
        "d": 1768521600000,
        "r": "HK",
        "n": "Polyfair Holdings",
        "c": "8532",
        "v": "1:10"
      },
      {
        "d": 1768262400000,
        "r": "HK",
        "n": "China Supply Chain Holdings",
        "c": "3708",
        "v": "1:10"
      }
    ],
    "totalPages": 1,
    "totalElements": 3,
    "page": 0,
    "last": true,
    "size": 20
  }
}
```

1. [股票IPO股票市场

   提供专业的股票IPO信息API接口，覆盖A股、美股、港股等全球多个市场数据。实时获取新股申购代码、发行价格、中签率、上市日期、募资金额及招股说明书关键信息，助力您的打新策略与市场研究。](/zh-cn/rest-api/stocks/stock-ipo)
2. [股票市场实时成交

   提供低延迟、高可用的股票Tick级数据API，包含完整的逐笔成交、买卖盘口（十档行情）、实时买卖挂单及Level-2深度数据。数据源直接对接交易所，覆盖A股、美股、港股等全球主流市场。](/zh-cn/rest-api/stocks/stock-tick)
