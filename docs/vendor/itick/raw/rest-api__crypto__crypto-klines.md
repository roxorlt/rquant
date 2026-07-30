<!-- source: https://docs.itick.org/zh-cn/rest-api/crypto/crypto-klines.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/crypto/crypto-klines -->

---
title: 批量历史K线查询
description: 批量查询多个加密货币交易对的历史K线数据。支持从分钟线到月线的多时间周期的完整OHLCV数据，最高可回溯数年历史。数据格式标准统一。
keywords: 批量K线查询API, 历史K线数据下载, 多交易对K线接口, 加密货币批量OHLCV, 批量获取历史行情, 多币种历史数据批量下载, OHLCV历史数据归档
---

# 加密货币 API - 批量历史K线查询

GET /crypto/klines?region={region}&codes={code}&kType={kType}&limit={limit}&et={et}

## 请求参数

| 参数名称 | 描述                                                                                                 | 必填  |
| -------- | ---------------------------------------------------------------------------------------------------- | ----- |
| region   | 市场代码 BA,BT                                                                                       | true  |
| codes    | 货币代码，多个用英文逗号隔开，如： BTCUSDT,ETHUSDT                                                   | true  |
| kType    | K线类型(1 分钟K，2 5分钟K，3 15分钟K，4 30分钟K，5 1小时K，6 2小时K，7 4小时K，8 日K，9 周K，10 月K) | true  |
| limit    | K线数量                                                                                              | true  |
| et       | 截止时间戳 (为空时默认为当前时间戳)                                                                  | false |

## 响应参数

| 响应参数 | 参数类型 | 描述     |
| -------- | -------- | -------- |
| t        | number   | 时间戳   |
| o        | number   | 开盘价   |
| h        | number   | 最高价   |
| l        | number   | 最低价   |
| c        | number   | 收盘价   |
| v        | number   | 成交数量 |
| tu       | number   | 成交额   |

## 代码示例

```python
import requests

url = "https://api.itick.org/crypto/klines?region=BA&codes=BTCUSDT,ETHUSDT&kType=2&et=1751328000000&limit=10"

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
.url("https://api.itick.org/crypto/klines?region=BA&codes=BTCUSDT,ETHUSDT&kType=2&et=1751328000000&limit=10")
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

	url := "https://api.itick.org/crypto/klines?region=BA&codes=BTCUSDT,ETHUSDT&kType=2&et=1751328000000&limit=10"

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
  path: '/crypto/klines?region=BA&codes=BTCUSDT,ETHUSDT&kType=2&et=1751328000000&limit=10',
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
curl --location 'https://api.itick.org/crypto/klines?region=BA&codes=BTCUSDT,ETHUSDT&kType=2&et=1751328000000&limit=10' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "BTCUSDT": [
      {
        "tu": 1436796.7075754,
        "c": 92474.15,
        "t": 1741239000000,
        "v": 15.54259,
        "h": 92500,
        "l": 92362,
        "o": 92426.53
      }
    ],
    "ETHUSDT": [
      {
        "tu": 1446224.424779,
        "c": 2313.91,
        "t": 1741239000000,
        "v": 625.2005,
        "h": 2318.49,
        "l": 2307.5,
        "o": 2309.89
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

klines = client.get_crypto_klines("BA", "BTCUSDT,ETHUSDT", 2, 10)
print(klines)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var klines = client.getCryptoKlines("BA", "BTCUSDT,ETHUSDT", 2, 10, null);
            System.out.println(klines);
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

	klines, err := client.GetCryptoKlines("BA", "BTCUSDT,ETHUSDT", 2, 10, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Crypto Klines: %+v\n", klines)
}
```

### Node.js SDK

```javascript
import { CryptoClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getKlines({
  region: "BA",
  codes: ["BTCUSDT", "ETHUSDT"],
  interval: "5m",
  limit: 10,
  et: 1751328000000,
});
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { CryptoClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new CryptoClient(token);

const res = await client.getKlines({
  region: "BA",
  codes: ["BTCUSDT", "ETHUSDT"],
  interval: "5m",
  limit: 10,
  et: 1751328000000,
});
console.log(res);
```

### MCP Server

通过 MCP Server 调用加密货币批量历史K线 API：

```python
# 使用 MCP 工具 cryptoKlines
# 对应 REST API: GET /crypto/klines
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
