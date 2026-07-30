<!-- source: https://docs.itick.org/zh-cn/rest-api/indices/indices-quote.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/indices/indices-quote -->

---
title: 实时报价
description: 全球主要股票指数的实时报价数据，覆盖沪深300、上证指数、深证成指、标普500、道琼斯、纳斯达克、恒生等核心指数。提供最新的指数价格、涨跌幅、成交量、开盘价、收盘价等完整行情信息，实时更新。
keywords: 指数实时报价API, 股票指数行情数据, 指数报价接口, 实时指数行情, 指数行情API, 上沪深300实时行情, 标普500实时报, 指数成交量数据, 全球指数行情, 指数涨跌幅数据
---

# 指数 API - 实时报价

GET /indices/quote?region={region}&code={code}

## 请求参数

| 参数名称 | 描述        | 必填 |
| -------- | ----------- | ---- |
| region   | 市场代码 GB | true |
| code     | 产品代码    | true |

## 响应参数

| 响应参数 | 参数类型 | 描述         |
| -------- | -------- | ------------ |
| s        | string   | 产品代码     |
| ld       | number   | 最新价       |
| l        | number   | 最低价       |
| o        | number   | 开盘价       |
| p        | number   | 前日收盘价   |
| h        | number   | 最高价       |
| t        | number   | 时间戳       |
| v        | number   | 成交数量     |
| tu       | number   | 成交额       |
| ts       | number   | 标的交易状态 |
| ch       | number   | 涨跌额       |
| chp      | number   | 涨跌幅百分比 |

## 代码示例

```python
import requests

url = "https://api.itick.org/indices/quote?region=GB&code=SPX"

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
.url("https://api.itick.org/indices/quote?region=GB&code=SPX")
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

	url := "https://api.itick.org/indices/quote?region=GB&code=SPX"

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
  path: '/indices/quote?region=GB&code=SPX',
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
curl --location 'https://api.itick.org/indices/quote?region=GB&code=SPX' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "s": "SPX",
    "ld": 6827.42,
    "o": 6886.85,
    "p": 6886.85,
    "h": 6899.85,
    "l": 6801.79,
    "t": 1765573268000,
    "v": 3086000000,
    "tu": 21106572620000,
    "ch": -73.59,
    "chp": -1.07,
    "ts": 0
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

quote = client.get_indices_quote("GB", "SPX")
print(quote)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var quote = client.getIndicesQuote("GB", "SPX");
            System.out.println(quote);
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

	quote, err := client.GetIndicesQuote("GB", "SPX")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Indices Quote: %+v\n", quote)
}
```

### Node.js SDK

```javascript
import { IndicesClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getQuote({ region: "GB", code: "SPX" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getQuote({ region: "GB", code: "SPX" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用指数实时报价 API：

```python
# 使用 MCP 工具 indicesQuote
# 对应 REST API: GET /indices/quote
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
