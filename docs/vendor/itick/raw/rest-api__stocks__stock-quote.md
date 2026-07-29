<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-quote.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-quote -->

---
title: 实时报价
description: 提供全球主流股票的实时报价数据，覆盖A股、美股、港股等全球市场数千只个股。包含最新价、涨跌幅、成交量、换手率、市盈率等完整行情指标，数据实时更新。
keywords: 股票实时报价, 实时行情API, 股票价格接口, 最新股价, 股市行情数据, 实时涨跌幅, A股实时行情, 港股实时报价, 美股行情API, 免费行情接口
---

# 股票 API - 实时报价

GET /stock/quote?region={region}&code={code}

## 请求参数

| 参数名称 | 描述                                                                                    | 必填 |
| -------- | --------------------------------------------------------------------------------------- | ---- |
| region   | 市场代码 HK、SZ、SH、US、SG、JP、TW、IN、TH、DE、MX、MY、TR、ES、NL、GB、ID、VN、IT | true |
| exchange | 上市交易所（可选）。相同市场存在多个股票产品时，可通过 exchange 区分查询；为空时返回主要上市交易所信息。exchange 字典可参考接口 [产品清单](/zh-cn/rest-api/basics/symbol-list) 返回的股票清单 | false |
| code     | 产品代码                                                                                | true |

## 响应参数

| 响应参数 | 参数类型 | 描述         |
| -------- | -------- | ------------ |
| s        | string   | 产品代码     |
| ld       | number   | 最新价       |
| l        | number   | 最低价       |
| o        | number   | 开盘价       |
| p        | number   | 前日收盘价   |
| h        | number   | 最高价       |
| t        | number   | 时间戳       |
| v        | number   | 成交数量     |
| tu       | number   | 成交额       |
| ts       | number   | 标的交易状态 0:正常交易 1:停牌 2:退市 3:熔断 |
| ch       | number   | 涨跌额       |
| chp      | number   | 涨跌幅百分比 |

## 代码示例

```python
import requests

url = "https://api.itick.org/stock/quote?region=HK&code=700"

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
.url("https://api.itick.org/stock/quote?region=HK&code=700")
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

	url := "https://api.itick.org/stock/quote?region=HK&code=700"

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
  path: '/stock/quote?region=HK&code=700',
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
curl --location 'https://api.itick.org/stock/quote?region=HK&code=700' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
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
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

quote = client.get_stock_quote("HK", "700")
print(quote)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var quote = client.getStockQuote("HK", "700");
            System.out.println(quote);
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

	quote, err := client.GetStockQuote("HK", "700")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock Quote: %+v\n", quote)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getQuote({ region: "HK", code: "700" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getQuote({ region: "HK", code: "700" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票实时报价 API：

```python
# 使用 MCP 工具 stockQuote
# 对应 REST API: GET /stock/quote
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
