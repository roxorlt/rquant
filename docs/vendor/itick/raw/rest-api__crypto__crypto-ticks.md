<!-- source: https://docs.itick.org/zh-cn/rest-api/crypto/crypto-ticks.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/crypto/crypto-ticks -->

---
title: 批量实时成交
description: 提供加密货币全市场的批量与历史Tick数据下载。可通过RESTful接口按时间范围批量获取毫秒级精度的完整订单簿变动流水，覆盖比特币、以太坊等数千交易对。数据包含逐笔成交与盘口快照。
keywords: 加密货币批量Tick数据, 数字货币历史Tick数据, 加密货币高频数据API, 批量行情数据下载, 加密货币全量Tick, 比特币历史逐笔数据, 高频交易历史数据
---

# 加密货币 API - 批量实时成交

GET /crypto/ticks?region={symbol}&codes={codes}

## 请求参数

| 响应参数 | 描述                      | 必填 |
| -------- | ------------------------- | ---- |
| region   | 市场代码 BA,BT            | true |
| codes    | 产品代码(BTCUSDT,ETHUSDT) | true |

## 响应参数

| 响应参数 | 参数类型 | 描述             |
| -------- | -------- | ---------------- |
| s        | string   | 标的代码         |
| ld       | number   | 最新价           |
| t        | number   | 最新成交的时间戳 |
| v        | number   | 成交数量         |
| tu       | number   | 成交额           |
| ts       | number   | 标的交易状态     |

## 代码示例

```python
import requests

url = "https://api.itick.org/crypto/ticks?region=BA&codes=BTCUSDT,ETHUSDT"

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
.url("https://api.itick.org/crypto/ticks?region=BA&codes=BTCUSDT,ETHUSDT")
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

	url := "https://api.itick.org/crypto/ticks?region=BA&codes=BTCUSDT,ETHUSDT"

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
  path: '/crypto/ticks?region=BA&codes=BTCUSDT,ETHUSDT',
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
curl --location 'https://api.itick.org/crypto/ticks?region=BA&codes=BTCUSDT,ETHUSDT' \
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
      "ld": 116100.9,
      "t": 1754585474123,
      "v": 0.00992,
      "tu": 1151.720928,
      "ts": 0
    },
    "ETHUSDT": {
      "s": "ETHUSDT",
      "ld": 3802.52,
      "t": 1754585474610,
      "v": 0.0648,
      "tu": 246.403296,
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

ticks = client.get_crypto_ticks("BA", "BTCUSDT,ETHUSDT")
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

            var ticks = client.getCryptoTicks("BA", "BTCUSDT,ETHUSDT");
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

	ticks, err := client.GetCryptoTicks("BA", "BTCUSDT,ETHUSDT")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Crypto Ticks: %+v\n", ticks)
}
```

### Node.js SDK

```javascript
import { CryptoClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getTicks({ region: "BA", codes: ["BTCUSDT", "ETHUSDT"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getTicks({ region: "BA", codes: ["BTCUSDT", "ETHUSDT"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用加密货币批量实时成交 API：

```python
# 使用 MCP 工具 cryptoTicks
# 对应 REST API: GET /crypto/ticks
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
