<!-- source: https://docs.itick.org/zh-cn/rest-api/fund/fund-quotes.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/fund/fund-quotes -->

---
title: 批量实时报价
description: 提供场内ETF、LOF等基金的批量实时报价数据流，数据覆盖中国基金、美国基金、香港基金等市场，完整呈现多只基金的最新价、涨跌幅、成交量及IOPV净值估值等关键指标。数据持续实时更新，毫秒级延迟确保行情精准同步。
keywords: 基金批量实时Tick数据API, ETF批量实时行情, LOF基金数据, 货币基金行情, 基金IOPV数据, 场内基金API, 基金净值估算, 实时基金价格
---

# 基金 API - 批量实时报价

GET /fund/quotes?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                      | 必填 |
| -------- | ----------------------------------------- | ---- |
| region   | 市场代码 US                               | true |
| codes    | 货币代码，多个用英文逗号隔开，如：QQQ,IEF | true |

## 响应参数

| 响应参数 | 参数类型 | 描述             |
| -------- | -------- | ---------------- |
| s        | string   | 产品代码         |
| ld       | number   | 最新价           |
| o        | number   | 开盘价           |
| p        | number   | 前日收盘价        |
| h        | number   | 最高价           |
| l        | number   | 最低价           |
| t        | number   | 最新成交的时间戳 |
| v        | number   | 成交数量         |
| tu       | number   | 成交金额         |
| ts       | number   | 标的交易状态     |
| ch       | number   | 涨跌额       |
| chp      | number   | 涨跌幅百分比 |

## 代码示例

```python
import requests

url = "https://api.itick.org/fund/quotes?region=US&codes=QQQ,IEF"

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
.url("https://api.itick.org/fund/quotes?region=US&codes=QQQ,IEF")
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

	url := "https://api.itick.org/fund/quotes?region=US&codes=QQQ,IEF"

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
  path: '/fund/quotes?region=US&codes=QQQ,IEF',
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
curl --location 'https://api.itick.org/fund/quotes?region=US&codes=QQQ,IEF' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "QQQ": {
      "s": "QQQ",
      "ld": 613.7,
      "o": 622.08,
      "p": 622.08,
      "h": 623.54,
      "l": 611.36,
      "t": 1765573199000,
      "v": 71141919,
      "tu": 43822640541.4171,
      "ch": -11.88,
      "chp": -1.9,
      "ts": 0
    },
    "IEF": {
      "s": "IEF",
      "ld": 96.19,
      "o": 96.19,
      "p": 96.19,
      "h": 96.2726,
      "l": 96.17,
      "t": 1765573199000,
      "v": 7414510,
      "tu": 713319664.251565,
      "ch": -0.26,
      "chp": -0.27,
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

quotes = client.get_fund_quotes("US", "QQQ,IEF")
print(quotes)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var quotes = client.getFundQuotes("US", "QQQ,IEF");
            System.out.println(quotes);
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

	quotes, err := client.GetFundQuotes("US", "QQQ,IEF")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Fund Quotes: %+v\n", quotes)
}
```

### Node.js SDK

```javascript
import { FundClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getQuotes({ region: "US", codes: ["QQQ", "IEF"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FundClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getQuotes({ region: "US", codes: ["QQQ", "IEF"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用基金批量实时报价 API：

```python
# 使用 MCP 工具 fundQuotes
# 对应 REST API: GET /fund/quotes
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
