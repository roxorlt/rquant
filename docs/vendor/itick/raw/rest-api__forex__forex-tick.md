<!-- source: https://docs.itick.org/zh-cn/rest-api/forex/forex-tick.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/forex/forex-tick -->

---
title: 实时成交
description: 全球主流外汇货币对的实时成交数据，包含EUR、GBP、JPY、CHF等主要交易对的逐笔成交流水。数据包含成交价格、成交量、时间戳及买卖方向，毫秒级精度更新。
keywords: 外汇实时成交数据, 外汇Tick数据API, 实时外汇成交, 外汇逐笔交易数据, 外汇买卖方向数据, 外汇市场Tick数据, 高频交易数据源
---

# 外汇 API - 实时成交

GET /forex/tick?region={region}&code={code}

## 请求参数

| 参数名称 | 描述        | 必填 |
| -------- | ----------- | ---- |
| region   | 市场代码 GB | true |
| code     | 产品代码    | true |

## 响应参数

| 参数名称 | 参数类型 | 描述     |
| -------- | -------- | -------- |
| s        | string   | 产品代码 |
| ld       | number   | 最新价   |
| t        | number   | 时间戳   |
| v        | number   | 成交数量 |

## 代码示例

```python
import requests

url = "https://api.itick.org/forex/tick?region=GB&code=EURUSD"

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
.url("https://api.itick.org/forex/tick?region=GB&code=EURUSD")
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

	url := "https://api.itick.org/forex/tick?region=GB&code=EURUSD"

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
  path: '/forex/tick?region=GB&code=EURUSD',
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
curl --location 'https://api.itick.org/forex/tick?region=GB&code=EURUSD' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "s": "EURUSD",
    "ld": 1.16429,
    "t": 1754583901037,
    "v": 1.8
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

tick = client.get_forex_tick("GB", "EURUSD")
print(tick)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var tick = client.getForexTick("GB", "EURUSD");
            System.out.println(tick);
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

	tick, err := client.GetForexTick("GB", "EURUSD")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Forex Tick: %+v\n", tick)
}
```

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getTick({ region: "GB", code: "EURUSD" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getTick({ region: "GB", code: "EURUSD" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用外汇实时成交 API：

```python
# 使用 MCP 工具 forexTick
# 对应 REST API: GET /forex/tick
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
