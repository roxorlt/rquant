<!-- source: https://docs.itick.org/zh-cn/rest-api/forex/forex-quotes.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/forex/forex-quotes -->

---
title: 批量实时报价
description: 批量获取全球主流外汇货币对的实时报价，提供获取多个外汇货币对的实时报价数据API，覆盖EUR、GBP、JPY、CHF等主要货币对。返回各货币对的实时买入价、卖出价、中间价及点差等完整报价信息，更新频率高。
keywords: 外汇批量报价API, 多货币对实时报价, 外汇批量行情接口, 实时汇率批量获取, 投资组合报价API, 多货币对实时报价, 外汇点差批量数据
---

# 外汇 API - 批量实时报价

GET /forex/quotes?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                             | 必填 |
| -------- | ------------------------------------------------ | ---- |
| region   | 市场代码 GB                                      | true |
| codes    | 货币代码，多个用英文逗号隔开，如： EURUSD,GBPUSD | true |

## 响应参数

| 响应参数 | 参数类型 | 描述             |
| -------- | -------- | ---------------- |
| s        | string   | 产品代码         |
| ld       | number   | 最新价           |
| o        | number   | 开盘价           |
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

url = "https://api.itick.org/forex/quotes?region=GB&codes=EURUSD,GBPUSD"

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
.url("https://api.itick.org/forex/quotes?region=GB&codes=EURUSD,GBPUSD")
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

	url := "https://api.itick.org/forex/quotes?region=GB&codes=EURUSD,GBPUSD"

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
  path: '/forex/quotes?region=GB&codes=EURUSD,GBPUSD',
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
curl --location 'https://api.itick.org/forex/quotes?region=GB&codes=EURUSD,GBPUSD' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "EURUSD": {
      "s": "EURUSD",
      "ld": 1.17376,
      "o": 1.1741,
      "h": 1.17497,
      "l": 1.17194,
      "t": 1765576744049,
      "v": 1193417.3,
      "tu": 1400561.87122,
      "ch": 0.00001,
      "chp": 0.02,
      "ts": 0
    },
    "GBPUSD": {
      "s": "GBPUSD",
      "ld": 1.33643,
      "o": 1.3393,
      "h": 1.33998,
      "l": 1.33422,
      "t": 1765576736076,
      "v": 1561368.1,
      "tu": 2088514.87546,
      "ch": -0.0019,
      "chp": -0.14,
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

quotes = client.get_forex_quotes("GB", "EURUSD,GBPUSD")
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

            var quotes = client.getForexQuotes("GB", "EURUSD,GBPUSD");
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

	quotes, err := client.GetForexQuotes("GB", "EURUSD,GBPUSD")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Forex Quotes: %+v\n", quotes)
}
```

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getQuotes({ region: "GB", codes: ["EURUSD", "GBPUSD"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getQuotes({ region: "GB", codes: ["EURUSD", "GBPUSD"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用外汇批量实时报价 API：

```python
# 使用 MCP 工具 forexQuotes
# 对应 REST API: GET /forex/quotes
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
