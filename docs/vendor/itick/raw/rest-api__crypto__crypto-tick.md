<!-- source: https://docs.itick.org/zh-cn/rest-api/crypto/crypto-tick.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/crypto/crypto-tick -->

---
title: 实时成交
description: 覆盖比特币(BTC)、以太坊(ETH)等数千种代币的最新价格、买一卖一、24小时成交量等深度字段。低延迟、高频率，为您的量化交易、行情看板与DApp提供核心数据支持。免费试用。
keywords: 实时成交, 加密货币, 实时Tick数据, 币种实时成交, 币种实时K线, 加密货币实时K线
---

# 加密货币 API - 实时成交

GET /crypto/tick?region={symbol}&code={code}

## 请求参数

| 参数名称 | 描述             | 必填 |
| -------- | ---------------- | ---- |
| region   | 市场代码 BA,BT   | true |
| code     | 产品代码 BTCUSDT | true |

## 响应参数

| 参数名称 | 参数类型 | 描述         |
| -------- | -------- | ------------ |
| s        | string   | 产品代码     |
| ld       | number   | 最新价       |
| t        | number   | 时间戳       |
| v        | number   | 成交数量     |
| tu       | number   | 成交额       |
| ts       | number   | 标的交易状态 |

## 代码示例

```python
import requests

url = "https://api.itick.org/crypto/tick?region=BA&code=BTCUSDT"

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
.url("https://api.itick.org/crypto/tick?region=BA&code=BTCUSDT")
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

	url := "https://api.itick.org/crypto/tick?region=BA&code=BTCUSDT"

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
  path: '/crypto/tick?region=BA&code=BTCUSDT',
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
curl --location 'https://api.itick.org/crypto/tick?region=BA&code=BTCUSDT' \
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
    "ld": 116101.99,
    "t": 1754585108147,
    "v": 0.36735,
    "tu": 42649.3395843,
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

tick = client.get_crypto_tick("BA", "BTCUSDT")
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

            var tick = client.getCryptoTick("BA", "BTCUSDT");
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

	tick, err := client.GetCryptoTick("BA", "BTCUSDT")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Crypto Tick: %+v\n", tick)
}
```

### Node.js SDK

```javascript
import { CryptoClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getTick({ region: "BA", code: "BTCUSDT" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getTick({ region: "BA", code: "BTCUSDT" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用加密货币实时成交 API：

```python
# 使用 MCP 工具 cryptoTick
# 对应 REST API: GET /crypto/tick
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
