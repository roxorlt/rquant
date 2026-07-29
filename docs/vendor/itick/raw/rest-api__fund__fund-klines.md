<!-- source: https://docs.itick.org/zh-cn/rest-api/fund/fund-klines.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/fund/fund-klines -->

---
title: 批量历史K线查询
description: 提供场内ETF、LOF等基金的批量K线数据流，完整呈现多只基金的开盘价、最高价、最低价、收盘价及成交量等OHLC指标。支持从分钟线到月线的多周期查询，数据经过复权处理确保连续性。
keywords: 基金批量K线API, 多基金K线数据, ETF OHLC数据, LOF历史行情, 基金行情接口, 基金复权数据, 场内基金K线, 基金分钟线数据
---

# 基金 API - 批量历史K线查询

GET /fund/klines?region={region}&codes={codes}&kType={kType}&limit={limit}&et={et}

## 请求参数

| 参数名称 | 描述                                                                                                 | 必填  |
| -------- | ---------------------------------------------------------------------------------------------------- | ----- |
| region   | 市场代码 US                                                                                          | true  |
| codes    | 产品代码，多个用英文逗号隔开, QQQ,IEF                                                                | true  |
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

url = "https://api.itick.org/fund/klines?region=US&codes=QQQ,IEF&kType=2&limit=5"

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
.url("https://api.itick.org/fund/klines?region=US&codes=QQQ,IEF&kType=2&limit=5")
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

	url := "https://api.itick.org/fund/klines?region=US&codes=QQQ,IEF&kType=2&limit=5"

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
  path: '/fund/klines?region=US&codes=QQQ,IEF&kType=2&limit=5',
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
curl --location 'https://api.itick.org/fund/klines?region=US&codes=QQQ,IEF&kType=2&limit=5' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "IEF": [
      {
        "tu": 956,
        "c": 95.6,
        "t": 1754608800000,
        "v": 10,
        "h": 95.6,
        "l": 95.6,
        "o": 95.6
      }
    ],
    "QQQ": [
      {
        "tu": 4813493.44,
        "c": 569.24,
        "t": 1754610900000,
        "v": 8456,
        "h": 569.24,
        "l": 569.24,
        "o": 569.24
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

klines = client.get_fund_klines("US", "QQQ,IEF", 2, 5)
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

            var klines = client.getFundKlines("US", "QQQ,IEF", 2, 5, null);
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

	klines, err := client.GetFundKlines("US", "QQQ,IEF", 2, 5, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Fund Klines: %+v\n", klines)
}
```

### Node.js SDK

```javascript
import { FundClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getKlines({
  region: "US",
  codes: ["QQQ", "IEF"],
  interval: "5m",
  limit: 5,
});
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FundClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getKlines({
  region: "US",
  codes: ["QQQ", "IEF"],
  interval: "5m",
  limit: 5,
});
console.log(res);
```

### MCP Server

通过 MCP Server 调用基金批量历史K线 API：

```python
# 使用 MCP 工具 fundKlines
# 对应 REST API: GET /fund/klines
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
