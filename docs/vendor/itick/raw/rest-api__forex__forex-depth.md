<!-- source: https://docs.itick.org/zh-cn/rest-api/forex/forex-depth.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/forex/forex-depth -->

---
title: 实时盘口
description: 全球主流外汇货币对的实时盘口，包含EUR、GBP、JPY、CHF等主要货币对的多档买卖价格与挂单量。数据实时更新，展现完整的市场流动性深度。
keywords: 外汇实时盘口, 外汇市场深度API, 外汇买卖盘口, 外汇多档买卖价格, 外汇挂单量数据, 实时外汇市场深度, 高频交易数据
---

# 外汇 API - 实时盘口

GET /forex/depth?region={region}&code={code}

## 请求参数

| 参数名称 | 描述             | 必填 |
| -------- | ---------------- | ---- |
| region   | 市场代码 GB      | true |
| code     | 产品代码 EURUSD | true |

## 响应参数

| 响应参数 | 参数类型      | 描述     |
| -------- | ------------- | -------- |
| s        | string        | 产品代码 |
| b        | array(object) | 买盘     |
| a        | array(object) | 卖盘     |
| po       | integer       | 档位     |
| p        | number        | 价格     |
| v        | number        | 挂单量   |
| o        | number        | 订单数量 |

## 代码示例

```python
import requests

url = "https://api.itick.org/forex/depth?region=GB&code=EURUSD"

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
.url("https://api.itick.org/forex/depth?region=GB&code=EURUSD")
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

	url := "https://api.itick.org/forex/depth?region=GB&code=EURUSD"

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
  path: '/forex/depth?region=GB&code=EURUSD',
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
curl --location 'https://api.itick.org/forex/depth?region=GB&code=EURUSD' \
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
    "a": [
      {
        "po": 1,
        "p": 1.16423,
        "v": 0,
        "o": 1
      }
    ],
    "b": [
      {
        "po": 1,
        "p": 1.16421,
        "v": 0,
        "o": 1
      }
    ]
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

depth = client.get_forex_depth("GB", "EURUSD")
print(depth)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var depth = client.getForexDepth("GB", "EURUSD");
            System.out.println(depth);
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

	depth, err := client.GetForexDepth("GB", "EURUSD")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Forex Depth: %+v\n", depth)
}
```

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getDepth({ region: "GB", code: "EURUSD" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getDepth({ region: "GB", code: "EURUSD" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用外汇实时盘口 API：

```python
# 使用 MCP 工具 forexDepth
# 对应 REST API: GET /forex/depth
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
