<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-quotes.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-quotes -->

---
title: 批量实时报价
description: 批量获取多个股票的实时报价数据，覆盖A股、美股、港股等多个全球主流市场。提供各股票的最新价、涨跌幅、成交量、成交额、换手率等完整行情指标，毫秒级延迟确保数据时效性。
keywords: 批量股票API, 实时报价接口, 多股票行情, 股票列表报价, 批量行情查询, A股批量报价, 港股实时行情, 实时价格批量获取, 多股实时盯盘, 并发行情接口
---

# 股票 API - 批量实时报价

GET /stock/quotes?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                                                                    | 必填 |
| -------- | --------------------------------------------------------------------------------------- | ---- |
| region   | 市场代码 HK、SZ、SH、US、SG、JP、TW、IN、TH、DE、MX、MY、TR、ES、NL、GB、ID、VN、IT | true |
| codes    | 货币代码，多个用英文逗号隔开，如： 700,9988                                             | true |
| exchange | 上市交易所（可选）。相同市场存在多个股票产品时，可通过 exchange 区分查询；为空时返回主要上市交易所信息。exchange 字典可参考接口 [产品清单](/zh-cn/rest-api/basics/symbol-list) 返回的股票清单 | false |

## 响应参数

| 响应参数 | 参数类型 | 描述             |
| -------- | -------- | ---------------- |
| s        | string   | 产品代码         |
| ld       | number   | 最新价           |
| o        | number   | 开盘价           |
| p        | number   | 前日收盘价       |
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

url = "https://api.itick.org/stock/quotes?region=HK&codes=700,9988"

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
.url("https://api.itick.org/stock/quotes?region=HK&codes=700,9988")
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

	url := "https://api.itick.org/stock/quotes?region=HK&codes=700,9988"

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
  path: '/stock/quotes?region=HK&codes=700,9988',
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
curl --location 'https://api.itick.org/stock/quotes?region=HK&codes=700,9988' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "700": {
      "s": "700",
      "ld": 616,
      "o": 608,
      "p": 608,
      "h": 616,
      "l": 601.5,
      "t": 1765526889000,
      "v": 17825495,
      "tu": 10871536434.36,
      "ts": 0,
      "ch": 8,
      "chp": 1.32
    },
    "9988": {
      "s": "9988",
      "ld": 154.1,
      "o": 152.7,
      "p": 152.7,
      "h": 154.1,
      "l": 150.8,
      "t": 1765526889000,
      "v": 88532981,
      "tu": 13539596437.06,
      "ts": 0,
      "ch": 1.4,
      "chp": 0.92
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

quotes = client.get_stock_quotes("HK", "700,9988")
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

            var quotes = client.getStockQuotes("HK", "700,9988");
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

	quotes, err := client.GetStockQuotes("HK", "700,9988")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock Quotes: %+v\n", quotes)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getQuotes({ region: "HK", codes: ["700", "9988"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getQuotes({ region: "HK", codes: ["700", "9988"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票批量实时报价 API：

```python
# 使用 MCP 工具 stockQuotes
# 对应 REST API: GET /stock/quotes
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
