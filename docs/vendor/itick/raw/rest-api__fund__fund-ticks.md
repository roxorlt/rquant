<!-- source: https://docs.itick.org/zh-cn/rest-api/fund/fund-ticks.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/fund/fund-ticks -->

---
title: 批量实时成交
description: 提供场内ETF、LOF等基金的批量实时Tick数据流，完整呈现多只基金的逐笔成交明细。支持从分钟线到月线的多时间周期, 数据包含精确至毫秒级的时间戳、成交价格、成交量及买卖方向，持续实时更新。
keywords: 基金批量Tick数据API, ETF逐笔成交, LOF交易数据, 基金行情API, 场内基金Tick, 基金IOPV数据, ETF套利数据, 基金Tick数据流
---

# 基金 API - 批量实时成交

GET /fund/ticks?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                     | 必填 |
| -------- | ---------------------------------------- | ---- |
| region   | 市场代码 US                              | true |
| codes    | 产品代码,多个用英文逗号隔开，如：QQQ,IEF | true |

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

url = "https://api.itick.org/fund/ticks?region=US&codes=QQQ,IEF"

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
.url("https://api.itick.org/fund/ticks?region=US&codes=QQQ,IEF")
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

	url := "https://api.itick.org/fund/ticks?region=US&codes=QQQ,IEF"

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
  path: '/fund/ticks?region=US&codes=QQQ,IEF',
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
curl --location 'https://api.itick.org/fund/ticks?region=US&codes=QQQ,IEF' \
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
      "ld": 553.88,
      "t": 1754092799000,
      "v": 11888
    },
    "IEF": {
      "s": "IEF",
      "ld": 95.68,
      "t": 1754091630000,
      "v": 10
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

ticks = client.get_fund_ticks("US", "QQQ,IEF")
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

            var ticks = client.getFundTicks("US", "QQQ,IEF");
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

	ticks, err := client.GetFundTicks("US", "QQQ,IEF")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Fund Ticks: %+v\n", ticks)
}
```

### Node.js SDK

```javascript
import { FundClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getTicks({ region: "US", codes: ["QQQ", "IEF"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FundClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getTicks({ region: "US", codes: ["QQQ", "IEF"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用基金批量实时成交 API：

```python
# 使用 MCP 工具 fundTicks
# 对应 REST API: GET /fund/ticks
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
