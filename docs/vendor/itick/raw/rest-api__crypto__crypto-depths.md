<!-- source: https://docs.itick.org/zh-cn/rest-api/crypto/crypto-depths.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/crypto/crypto-depths -->

---
title: 批量实时盘口
description: 批量获取多个加密货币交易对的实时盘口深度数据。返回各交易对完整的买卖盘口列表，包含多档价位与挂单量。毫秒级更新。
keywords: 批量实时盘口API, 批量订单簿数据, 多交易对盘口接口, 加密货币批量深度数据, 批量获取买卖盘口, 交易所批量深度接口
---

# 加密货币 API - 批量实时盘口

GET /crypto/depths?region={symbol}&codes={codes}

## 请求参数

| 参数名称 | 描述                                               | 必填 |
| -------- | -------------------------------------------------- | ---- |
| region   | 市场代码 BA,BT                                     | true |
| codes    | 货币代码，多个用英文逗号隔开，如： BTCUSDT,ETHUSDT | true |

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

url = "https://api.itick.org/crypto/depths?region=BA&codes=BTCUSDT,ETHUSDT"

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
.url("https://api.itick.org/crypto/depths?region=BA&codes=BTCUSDT,ETHUSDT")
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

	url := "https://api.itick.org/crypto/depths?region=BA&codes=BTCUSDT,ETHUSDT"

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
  path: '/crypto/depths?region=BA&codes=BTCUSDT,ETHUSDT',
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
curl --location 'https://api.itick.org/crypto/depths?region=BA&codes=BTCUSDT,ETHUSDT' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "BTCUSDT": {
      "s": "BTCUSDT",
      "a": [
        {
          "po": 1,
          "p": 116137.19,
          "v": 0.18474,
          "o": 0.18474
        },
        {
          "po": 2,
          "p": 116137.2,
          "v": 0.0006,
          "o": 0.0006
        },
        {
          "po": 3,
          "p": 116137.4,
          "v": 0.00005,
          "o": 0.00005
        },
        {
          "po": 4,
          "p": 116137.49,
          "v": 0.00005,
          "o": 0.00005
        },
        {
          "po": 5,
          "p": 116138,
          "v": 0.00005,
          "o": 0.00005
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 116137.18,
          "v": 8.24124,
          "o": 8.24124
        },
        {
          "po": 2,
          "p": 116136,
          "v": 0.05295,
          "o": 0.05295
        },
        {
          "po": 3,
          "p": 116135.42,
          "v": 0.00042,
          "o": 0.00042
        },
        {
          "po": 4,
          "p": 116134.52,
          "v": 0.31005,
          "o": 0.31005
        },
        {
          "po": 5,
          "p": 116134.15,
          "v": 0.00005,
          "o": 0.00005
        }
      ]
    },
    "ETHUSDT": {
      "s": "ETHUSDT",
      "a": [
        {
          "po": 1,
          "p": 3811.26,
          "v": 27.1743,
          "o": 27.1743
        },
        {
          "po": 2,
          "p": 3811.27,
          "v": 0.4375,
          "o": 0.4375
        },
        {
          "po": 3,
          "p": 3811.28,
          "v": 0.0241,
          "o": 0.0241
        },
        {
          "po": 4,
          "p": 3811.29,
          "v": 0.0014,
          "o": 0.0014
        },
        {
          "po": 5,
          "p": 3811.3,
          "v": 0.0382,
          "o": 0.0382
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 3811.25,
          "v": 132.6926,
          "o": 132.6926
        },
        {
          "po": 2,
          "p": 3811.24,
          "v": 2.0021,
          "o": 2.0021
        },
        {
          "po": 3,
          "p": 3811.05,
          "v": 4,
          "o": 4
        },
        {
          "po": 4,
          "p": 3811.03,
          "v": 1.6119,
          "o": 1.6119
        },
        {
          "po": 5,
          "p": 3810.99,
          "v": 12,
          "o": 12
        }
      ]
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

depths = client.get_crypto_depths("BA", "BTCUSDT,ETHUSDT")
print(depths)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var depths = client.getCryptoDepths("BA", "BTCUSDT,ETHUSDT");
            System.out.println(depths);
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

	depths, err := client.GetCryptoDepths("BA", "BTCUSDT,ETHUSDT")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Crypto Depths: %+v\n", depths)
}
```

### Node.js SDK

```javascript
import { CryptoClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getDepths({ region: "BA", codes: ["BTCUSDT", "ETHUSDT"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getDepths({ region: "BA", codes: ["BTCUSDT", "ETHUSDT"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用加密货币批量实时盘口 API：

```python
# 使用 MCP 工具 cryptoDepths
# 对应 REST API: GET /crypto/depths
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
