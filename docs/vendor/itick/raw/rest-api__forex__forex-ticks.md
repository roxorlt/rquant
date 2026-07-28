<!-- source: https://docs.itick.org/zh-cn/rest-api/forex/forex-ticks.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/forex/forex-ticks -->

---
title: 批量实时成交
description: 批量获取全球主流外汇货币对的实时Tick数据，包含EUR、GBP、JPY、CHF等多个货币对的逐笔成交流水。数据包含精确时间戳、成交价格、成交量及买卖方向，毫秒级更新精度。
keywords: 外汇批量Tick数据, 外汇实时Tick API, 外汇高频数据接口, 货币对Tick数据, 外汇成交数据, 外汇逐笔成交流水, EUR/USD实时Tick
---

# 外汇 API - 批量实时成交

GET /forex/ticks?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                            | 必填 |
| -------- | ----------------------------------------------- | ---- |
| region   | 市场代码 GB                                     | true |
| codes    | 产品代码,多个用英文逗号隔开，如： EURUSD,GBPUSD | true |

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

url = "https://api.itick.org/forex/ticks?region=GB&codes=EURUSD,GBPUSD"

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
.url("https://api.itick.org/forex/ticks?region=GB&codes=EURUSD,GBPUSD")
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

	url := "https://api.itick.org/forex/ticks?region=GB&codes=EURUSD,GBPUSD"

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
  path: '/forex/ticks?region=GB&codes=EURUSD,GBPUSD',
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
curl --location 'https://api.itick.org/forex/ticks?region=GB&codes=EURUSD,GBPUSD' \
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
      "ld": 1.16425,
      "t": 1754583972000,
      "v": 3
    },
    "GBPUSD": {
      "s": "GBPUSD",
      "ld": 1.34292,
      "t": 1754583971255,
      "v": 6.3
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

ticks = client.get_forex_ticks("GB", "EURUSD,GBPUSD")
print(ticks)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var ticks = client.getForexTicks("GB", "EURUSD,GBPUSD");
            System.out.println(ticks);
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

	ticks, err := client.GetForexTicks("GB", "EURUSD,GBPUSD")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Forex Ticks: %+v\n", ticks)
}
```

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getTicks({ region: "GB", codes: ["EURUSD", "GBPUSD"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getTicks({ region: "GB", codes: ["EURUSD", "GBPUSD"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用外汇批量实时成交 API：

```python
# 使用 MCP 工具 forexTicks
# 对应 REST API: GET /forex/ticks
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
