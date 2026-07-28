<!-- source: https://docs.itick.org/zh-cn/rest-api/news/list -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: html | slug: rest-api/news/list -->

# 免费金融市场API 实时新闻 API 文档｜财经新闻与金融数据 API 金融市场新闻资讯API接口 REST API 查询指定市场与语言下的实时新闻，返回标题、发布时间、原文链接、供应商

1. [文档](/zh-cn)
2. [REST API](/zh-cn/rest-api/basics/symbol-list)
3. [新闻资讯](/zh-cn/rest-api/news/list)
4. [实时新闻](/zh-cn/rest-api/news/list)

Copy

API KEY

请选择API KEY

* 立即登录/注册

## 实时新闻查询

GET

/news

### 请求参数

marketenum必填

股票

* 股票
* 外汇
* 指数
* 加密货币
* 期货
* 基金

市场类型，取值来自产品类型配置

langenum必填

中文繁体

* 中文简体
* English
* 中文繁体

新闻语言

limitstring

必填字段

查询数量

### 响应参数

codenumber

响应code

msgstring

响应描述

dataarray(object)

响应结果

idstring

新闻 ID

testring

标题

ptstring

发布时间，Unix 时间戳

mtstring

市场类型

lgstring

语言

lkstring

原始文章链接

pvstring

供应商

### 代码示例

```
import requests

url = "https://api.itick.org/news?market=stock&lang=hk&limit=2"

headers = {
"accept": "application/json"
"token": "Your Token"
}

response = requests.get(url, headers=headers)

print(response.text)
```

### 查询 URL

GET

https://api.itick.org/news?code=HK

### 响应结果

响应示例查询结果

```
{
  "code": 0,
  "msg": "ok",
  "data": {
    "data": [
      {
        "id": "9de18b22-7c7d-3142-9082-de39d3dc7e1a",
        "te": "拼多多雄安公司員工數量超600人，成為新區最大互聯網民營企業",
        "pt": 1783264154,
        "mt": "stock",
        "lg": "hk",
        "lk": "https://www.gelonghui.com/live/2537364",
        "pv": "Gelonghui"
      },
      {
        "id": "bb7af54f-418c-32a6-88a7-3791735b3c92",
        "te": "郭明錤稱摺疊iPhone可能重演iPhone X劇本：同場發佈、較晚開賣，供應吃緊延續至年底",
        "pt": 1783262706,
        "mt": "stock",
        "lg": "hk",
        "lk": "https://www.gelonghui.com/live/2537362",
        "pv": "Gelonghui"
      }
    ],
    "pagination": {
      "nextCursor": "2026-07-05T14:45:06.000690825Z",
      "hasMore": true,
      "count": 2
    }
  }
}
```

1. [批量K线查询基金市场

   提供场内ETF、LOF等基金的批量K线数据流，完整呈现多只基金的开盘价、最高价、最低价、收盘价及成交量等OHLC指标。支持从分钟线到月线的多周期查询，数据经过复权处理确保连续性。](/zh-cn/rest-api/fund/fund-klines)
2. [新闻详情

   根据新闻 ID 查询新闻详情，返回标题、发布时间、原文链接、描述、正文、标签与市场信息](/zh-cn/rest-api/news/detail)
