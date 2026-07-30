<!-- source: https://docs.itick.org/zh-cn/rest-api/future/future-quotes.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/future/future-quotes -->

---
title: 批量实时报价
description: 批量获取多个期货合约的实时报价数据，覆盖商品期货、金融期货等全品种主力合约与连续合约。提供各合约的最新价、涨跌幅、成交量、持仓量、买卖价等完整行情指标，毫秒级更新确保数据时效性。
keywords: 期货批量报价API, 多合约实时行情, 批量行情接口, 商品期货报价, 金融期货行情, 美国期货, 香港期货, 主力合约数据
---

# 期货 API - 批量实时报价

GET /future/quotes?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                    | 必填 |
| -------- | --------------------------------------- | ---- |
| region   | 市场代码 US、HK、CN                     | true |
| codes    | 货币代码，多个用英文逗号隔开，如：NQ,ES | true |

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

url = "https://api.itick.org/future/quotes?region=US&codes=NQ,ES"

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
.url("https://api.itick.org/future/quotes?region=US&codes=NQ,ES")
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

	url := "https://api.itick.org/future/quotes?region=US&codes=NQ,ES"

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
  path: '/future/quotes?region=US&codes=NQ,ES',
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
curl --location 'https://api.itick.org/future/quotes?region=US&codes=NQ,ES' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "NQ": {
      "s": "NQ",
      "ld": 25213.5,
      "o": 25674,
      "p": 25704.75,
      "h": 25703.75,
      "l": 25118,
      "t": 1765576802621,
      "v": 736750,
      "tu": 18670551589,
      "ch": -500,
      "chp": -1.94,
      "ts": 0
    },
    "ES": {
      "s": "ES",
      "ld": 6830.75,
      "o": 6904.25,
      "p": 6904.25,
      "h": 6913,
      "l": 6805,
      "t": 1765579830530,
      "v": 2464264,
      "tu": 16874645961.25,
      "ch": -76.5,
      "chp": -1.11,
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

quotes = client.get_future_quotes("US", "NQ,ES")
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

            var quotes = client.getFutureQuotes("US", "NQ,ES");
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

	quotes, err := client.GetFutureQuotes("US", "NQ,ES")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Future Quotes: %+v\n", quotes)
}
```

### Node.js SDK

```javascript
import { FutureClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getQuotes({ region: "US", codes: ["NQ", "ES"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FutureClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getQuotes({ region: "US", codes: ["NQ", "ES"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用期货批量实时报价 API：

```python
# 使用 MCP 工具 futureQuotes
# 对应 REST API: GET /future/quotes
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
