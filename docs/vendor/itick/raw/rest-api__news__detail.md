<!-- source: https://docs.itick.org/zh-cn/rest-api/news/detail -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: html | slug: rest-api/news/detail -->

# 免费金融市场API 新闻详情 API 文档｜财经新闻与金融数据 API 金融市场新闻资讯API接口 REST API 根据新闻 ID 查询新闻详情，返回标题、发布时间、原文链接、描述、正文、标签与市场信息

1. [文档](/zh-cn)
2. [REST API](/zh-cn/rest-api/basics/symbol-list)
3. [新闻资讯](/zh-cn/rest-api/news/list)
4. [新闻详情](/zh-cn/rest-api/news/detail)

Copy

API KEY

请选择API KEY

* 立即登录/注册

## 查询新闻资讯详细信息

GET

/news/{id}

### 请求参数

idstring

必填字段

新闻 ID，来自新闻列表接口返回的 id

langenum必填

中文繁体

* 中文简体
* English
* 中文繁体

新闻语言

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

sdstring

描述

cdstring

内容

tgarray(string)

标签

mtsarray(string)

所属市场

### 代码示例

```
import requests

url = "https://api.itick.org/news/3f5e9889-5c8a-335f-b8b4-faaaf741e967?lang=hk"

headers = {
"accept": "application/json"
"token": "Your Token"
}

response = requests.get(url, headers=headers)

print(response.text)
```

### 查询 URL

GET

https://api.itick.org/news/{id}?code=HK

### 响应结果

响应示例查询结果

```
{
  "code": 0,
  "msg": "ok",
  "data": {
    "id": "3f5e9889-5c8a-335f-b8b4-faaaf741e967",
    "te": "兗礦能源(600188.SH)：已回購196.52萬股公司A股股份",
    "pt": 1782984297,
    "lg": "hk",
    "lk": "https://www.gelonghui.com/news/5261924",
    "sd": "格隆匯7月2日丨兗礦能源SSE:600188公佈，2026年6月5日，公司通過集中競價交易方式回購公司A股股份196.52萬股，支付的資金總額為人民幣5092.53萬元（不含交易費用）。",
    "cd": "格隆匯7月2日丨兗礦能源SSE:600188公佈，2026年6月5日，公司通過集中競價交易方式回購公司A股股份196.52萬股，支付的資金總額為人民幣5092.53萬元（不含交易費用）。",
    "tg": [
      "Chinese stocks",
      "Gelonghui"
    ],
    "mts": [
      "stock"
    ]
  }
}
```

1. [实时新闻

   查询指定市场与语言下的实时新闻，返回标题、发布时间、原文链接、供应商](/zh-cn/rest-api/news/list)
2. [Websocket API加密货币

   提供全球主流加密货币最新数据的流式访问，实时推送比特币、以太坊等主流币种的Tick成交、K线更新、订单簿深度及聚合行情。支持多交易所行情聚合，毫秒级低延迟推送。](/zh-cn/websocket/crypto)
