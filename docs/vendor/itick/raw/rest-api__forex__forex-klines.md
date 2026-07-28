<!-- source: https://docs.itick.org/zh-cn/rest-api/forex/forex-klines.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/forex/forex-klines -->

---
title: 批量历史K线查询
description: 批量获取多个外汇货币对的历史与实时K线数据，覆盖EUR、GBP、JPY、CHF等多个货币的完整OHLC价格序列。支持从分钟线到月线的多时间周期, 提供标准化的时间戳、开盘价、最高价、最低价、收盘价数据。
keywords: 外汇批量K线API, 外汇OHLC批量数据, 货币对K线批量查询, 外汇历史价格批量, 外汇K线数据接口, 多货币对K线数据, EUR/USD历史K线
---

# 外汇 API - 批量历史K线查询

GET /forex/klines?region={region}&codes={codes}&kType={kType}&limit={limit}&et={et}

## 请求参数

| 参数名称 | 描述                                                                                                 | 必填  |
| -------- | ---------------------------------------------------------------------------------------------------- | ----- |
| region   | 市场代码 GB                                                                                          | true  |
| codes    | 产品代码，多个用英文逗号隔开，如： EURUSD,GBPUSD                                                     | true  |
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

url = "https://api.itick.org/forex/klines?region=GB&codes=EURUSD,GBPUSD&kType=2&limit=10"

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
.url("https://api.itick.org/forex/klines?region=GB&codes=EURUSD,GBPUSD&kType=2&limit=10")
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

	url := "https://api.itick.org/forex/klines?region=GB&codes=EURUSD,GBPUSD&kType=2&limit=10"

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
  path: '/forex/klines?region=GB&codes=EURUSD,GBPUSD&kType=2&limit=10',
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
curl --location 'https://api.itick.org/forex/klines?region=GB&codes=EURUSD,GBPUSD&kType=2&limit=10' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "EURUSD": [
      {
        "tu": 1108.4808,
        "c": 1.08058,
        "t": 1741239000000,
        "v": 1026,
        "h": 1.08064,
        "l": 1.08016,
        "o": 1.08023
      }
    ],
    "GBPUSD": [
      {
        "tu": 1131.40863,
        "c": 1.29028,
        "t": 1741239000000,
        "v": 877,
        "h": 1.29031,
        "l": 1.28982,
        "o": 1.28986
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

klines = client.get_forex_klines("GB", "EURUSD,GBPUSD", 2, 10)
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

            var klines = client.getForexKlines("GB", "EURUSD,GBPUSD", 2, 10, null);
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

	klines, err := client.GetForexKlines("GB", "EURUSD,GBPUSD", 2, 10, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Forex Klines: %+v\n", klines)
}
```

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getKlines({
  region: "GB",
  codes: ["EURUSD", "GBPUSD"],
  interval: "5m",
  limit: 10,
});
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getKlines({
  region: "GB",
  codes: ["EURUSD", "GBPUSD"],
  interval: "5m",
  limit: 10,
});
console.log(res);
```

### MCP Server

通过 MCP Server 调用外汇批量历史K线 API：

```python
# 使用 MCP 工具 forexKlines
# 对应 REST API: GET /forex/klines
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
