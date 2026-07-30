<!-- source: https://docs.itick.org/zh-cn/rest-api/crypto/crypto-depth.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/crypto/crypto-depth -->

---
title: 实时盘口
description: 加密货币的实时订单簿深度数据。提供完整的买卖盘口列表，包括各档位的价格与挂单量，支持L1至L50+档位深度。毫秒级更新的市场深度数据。
keywords: 加密货币订单簿API, 数字货币市场深度, 加密货币盘口数据, Order Book API, 加密货币深度数据, L2市场数据, 交易所深度数据聚合，买卖五档行情
---

# 加密货币 API - 实时盘口

GET /crypto/depth?region={symbol}&code={code}

## 请求参数

| 参数名称 | 描述             | 必填 |
| -------- | ---------------- | ---- |
| region   | 市场代码 BA,BT   | true |
| code     | 产品代码 BTCUSDT | true |

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

url = "https://api.itick.org/crypto/depth?region=BA&code=BTCUSDT"

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
.url("https://api.itick.org/crypto/depth?region=BA&code=BTCUSDT")
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

	url := "https://api.itick.org/crypto/depth?region=BA&code=BTCUSDT"

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
  path: '/crypto/depth?region=BA&code=BTCUSDT',
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
curl --location 'https://api.itick.org/crypto/depth?region=BA&code=BTCUSDT' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "s": "BTCUSDT",
    "a": [
      {
        "po": 1,
        "p": 116078.44,
        "v": 9.45753,
        "o": 9.45753
      },
      {
        "po": 2,
        "p": 116079.84,
        "v": 0.02786,
        "o": 0.02786
      },
      {
        "po": 3,
        "p": 116079.99,
        "v": 0.0001,
        "o": 0.0001
      },
      {
        "po": 4,
        "p": 116080,
        "v": 0.05315,
        "o": 0.05315
      },
      {
        "po": 5,
        "p": 116080.46,
        "v": 0.00042,
        "o": 0.00042
      }
    ],
    "b": [
      {
        "po": 1,
        "p": 116078.43,
        "v": 0.24692,
        "o": 0.24692
      },
      {
        "po": 2,
        "p": 116078.42,
        "v": 0.00065,
        "o": 0.00065
      },
      {
        "po": 3,
        "p": 116076.39,
        "v": 0.0001,
        "o": 0.0001
      },
      {
        "po": 4,
        "p": 116076.38,
        "v": 0.00015,
        "o": 0.00015
      },
      {
        "po": 5,
        "p": 116076.09,
        "v": 0.00005,
        "o": 0.00005
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

depth = client.get_crypto_depth("BA", "BTCUSDT")
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

            var depth = client.getCryptoDepth("BA", "BTCUSDT");
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

	depth, err := client.GetCryptoDepth("BA", "BTCUSDT")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Crypto Depth: %+v\n", depth)
}
```

### Node.js SDK

```javascript
import { CryptoClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getDepth({ region: "BA", code: "BTCUSDT" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getDepth({ region: "BA", code: "BTCUSDT" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用加密货币实时盘口 API：

```python
# 使用 MCP 工具 cryptoDepth
# 对应 REST API: GET /crypto/depth
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
