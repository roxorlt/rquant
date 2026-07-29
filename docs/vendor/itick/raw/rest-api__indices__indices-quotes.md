<!-- source: https://docs.itick.org/zh-cn/rest-api/indices/indices-quotes.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/indices/indices-quotes -->

---
title: 批量实时报价
description: 批量获取全球主要股票指数的实时报价数据，覆盖沪深300、上证指数、深证成指、标普500、纳斯达克、恒生等核心指数。提供各指数的最新价、涨跌幅、成交量、振幅等完整行情指标，毫秒级更新频率。
keywords: 指数批量报价API, 指数实时行情, 多指数数据接口, 指数行情批量获取, 指数涨跌幅数据, 多市场分析, 指数振幅数据, 量化投资
---

# 指数 API - 批量实时报价

GET /indices/quotes?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                       | 必填 |
| -------- | ------------------------------------------ | ---- |
| region   | 市场代码 GB                                | true |
| codes    | 货币代码，多个用英文逗号隔开，如：SPX,DJI | true |

## 响应参数

| 响应参数 | 参数类型 | 描述             |
| -------- | -------- | ---------------- |
| s        | string   | 产品代码         |
| ld       | number   | 最新价           |
| o        | number   | 开盘价           |
| p        | number   | 前日收盘价       |
| h        | number   | 最高价           |
| l        | number   | 最低价           |
| t        | number   | 最新成交的时间戳 |
| v        | number   | 成交数量         |
| tu       | number   | 成交金额         |
| ts       | number   | 标的交易状态     |
| ch       | number   | 涨跌额       |
| chp      | number   | 涨跌幅百分比 |

## 代码示例

```python
import requests

url = "https://api.itick.org/indices/quotes?region=GB&codes=SPX,DJI"

headers = {
"accept": "application/json"
"token": "your_token"
}

response = requests.get(url, headers=headers)

print(response.text)

```

```java
OkHttpClient client = new OkHttpClient();

Request request = new Request.Builder()
.url("https://api.itick.org/indices/quotes?region=GB&codes=SPX,DJI")
.get()
.addHeader("accept", "application/json")
.addHeader("token", "your_token")
.build();

Response response = client.newCall(request).execute();
```

```go
package main

import (
	"fmt"
	"net/http"
	"io"
)

func main() {

	url := "https://api.itick.org/indices/quotes?region=GB&codes=SPX,DJI"

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("token", "your_token")

	res, _ := http.DefaultClient.Do(req)

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	fmt.Println(string(body))

}
```

```javascript
const https = require('https');

const options = {
  method: 'GET',
  hostname: 'api.itick.org',
  path: '/indices/quotes?region=GB&codes=SPX,DJI',
  headers: {
    'accept': 'application/json',
    'token': '{token}'
  }
};

const req = https.request(options, function (res) {
  const chunks = [];

  res.on('data', function (chunk) {
    chunks.push(chunk);
  });

  res.on('end', function () {
    const body = Buffer.concat(chunks);
    console.log(body.toString());
  });
});

req.end();
```

```bash
curl --location 'https://api.itick.org/indices/quotes?region=GB&codes=SPX,DJI' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "SPX": {
      "s": "SPX",
      "ld": 6827.42,
      "o": 6886.85,
      "p": 6865.05,
      "h": 6899.85,
      "l": 6801.79,
      "t": 1765573268000,
      "v": 3086000000,
      "tu": 21106572620000,
      "ch": -73.59,
      "chp": -1.07,
      "ts": 0
    },
    "DJI": {
      "s": "DJI",
      "ld": 48458.06,
      "o": 48714.75,
      "p": 48309.05,
      "h": 48886.86,
      "l": 48334.1,
      "t": 1765574400000,
      "v": 992800000,
      "tu": 48155853266000,
      "ch": -245.96,
      "chp": -0.51,
      "ts": 0
    }
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

quotes = client.get_indices_quotes("GB", "SPX,DJI")
print(quotes)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var quotes = client.getIndicesQuotes("GB", "SPX,DJI");
            System.out.println(quotes);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Go SDK

```go
package main

import (
	"fmt"
	"log"

	"io.github.itick/sdk"
)

func main() {
	token := "your_api_token"
	client := sdk.NewClient(token)

	quotes, err := client.GetIndicesQuotes("GB", "SPX,DJI")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Indices Quotes: %+v\n", quotes)
}
```

### Node.js SDK

```javascript
import { IndicesClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getQuotes({ region: "GB", codes: ["SPX", "DJI"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getQuotes({ region: "GB", codes: ["SPX", "DJI"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用指数批量实时报价 API：

```python
# 使用 MCP 工具 indicesQuotes
# 对应 REST API: GET /indices/quotes
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
