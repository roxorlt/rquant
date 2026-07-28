<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-ipo.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-ipo -->

---
title: 股票IPO
description: 提供专业的股票IPO信息API接口，覆盖A股、美股、港股等全球多个市场数据。实时获取新股申购代码、发行价格、中签率、上市日期、募资金额及招股说明书关键信息，助力您的打新策略与市场研究。
keywords: IPO API, 新股数据接口, 上市信息API, 股票发行数据, 新股申购信息, 中签率查询, 招股书数据, 新股上市日期, A股IPO接口, 港股IPO数据, 美股新股上市
---

# 股票 API - 股票IPO

GET /stock/ipo?type={type}&region={region}

## 请求参数

| 参数名称 | 描述           | 必填 |
| -------- | -------------- | ---- |
| type     | 类型（upcoming 即将上市、recent 近期上市的股票） | true |
| region   | 市场代码 HK    | true |

## 响应参数

| 响应参数 | 参数类型 | 描述                        |
| -------- | -------- | --------------------------- |
| dt       | number   | 上市日期,时间戳，精确到毫秒 |
| cn       | number   | 股票公司名称                |
| sc       | number   | 股票代码                    |
| ex       | string   | 交易所名称                  |
| mc       | string   | 市值                        |
| pr       | string   | 价格                        |
| ct       | string   | 国家代码                    |
| bs       | number   | 申购开始时间,时间戳，秒     |
| es       | number   | 申购截止时间,时间戳，秒     |
| ro       | number   | 公布中签结果时间,时间戳，秒 |

## 代码示例

```python
import requests

url = "https://api.itick.org/stock/ipo?type=upcoming&region=HK"

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
.url("https://api.itick.org/stock/ipo?type=upcoming&region=HK")
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

	url := "https://api.itick.org/stock/ipo?type=upcoming&region=HK"

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
  path: '/stock/ipo?type=upcoming&region=HK',
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
curl --location 'https://api.itick.org/stock/ipo?type=upcoming&region=HK' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": "ok",
  "data": {
    "content": [
      {
        "dt": 1755820800000,
        "cn": "Picard Medical Inc",
        "sc": "PMI",
        "ex": "NYSE",
        "mc": "19.1M",
        "pr": "3.50-4.50",
        "ct": "US",
        "bs": 1648403200,
        "es": 1648998400,
        "ro": 1649001600
      },
      {
        "dt": 1755734400000,
        "cn": "Elite Express Holding Inc",
        "sc": "ETS",
        "ex": "NASDAQ",
        "mc": "16.0M",
        "pr": "4.00",
        "ct": "US",
        "bs": 1648403200,
        "es": 1648998400,
        "ro": 1649001600
      }
    ],
    "page": 0,
    "totalElements": 28,
    "totalPages": 14,
    "last": false,
    "size": 2
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

ipo = client.get_stock_ipo("upcoming", "HK")
print(ipo)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var ipo = client.getStockIPO("upcoming", "HK");
            System.out.println(ipo);
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

	ipo, err := client.GetStockIPO("upcoming", "HK")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock IPO: %+v\n", ipo)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getIPO({ type: "upcoming", region: "HK" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getIPO({ type: "upcoming", region: "HK" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票 IPO API：

```python
# 使用 MCP 工具 stockIpo
# 对应 REST API: GET /stock/ipo
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
