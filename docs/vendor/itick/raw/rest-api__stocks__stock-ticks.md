<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-ticks.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-ticks -->

---
title: 批量实时成交
description: 提供灵活可定制的批量实时Tick数据接口。您可自由创建和管理多个股票关注列表，API将仅推送您所订阅组合的实时逐笔成交与盘口变化。
keywords: 股票批量Tick数据API, 逐笔成交数据流, 批量实时Tick数据, 多股并发逐笔成交, 行情流, 高频交易数据, 量化交易接口
---

# 股票 API - 批量实时成交

GET /stock/ticks?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                                                                    | 必填 |
| -------- | --------------------------------------------------------------------------------------- | ---- |
| region   | 市场代码 HK、SZ、SH、US、SG、JP、TW、IN、TH、DE、MX、MY、TR、ES、NL、GB、ID、VN、IT | true |
| codes    | 产品代码,多个用英文逗号隔开，如： 700,9988                                              | true |
| exchange | 上市交易所（可选）。相同市场存在多个股票产品时，可通过 exchange 区分查询；为空时返回主要上市交易所信息。exchange 字典可参考接口 [产品清单](/zh-cn/rest-api/basics/symbol-list) 返回的股票清单 | false |

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

url = "https://api.itick.org/stock/ticks?region=HK&codes=700,9988"

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
.url("https://api.itick.org/stock/ticks?region=HK&codes=700,9988")
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

	url := "https://api.itick.org/stock/ticks?region=HK&codes=700,9988"

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
  path: '/stock/ticks?region=HK&codes=700,9988',
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
curl --location 'https://api.itick.org/stock/ticks?region=HK&codes=700,9988' \
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
      "ld": 567,
      "t": 1754554087000,
      "v": 1134500
    },
    "9988": {
      "s": "9988",
      "ld": 119.2,
      "t": 1754554087000,
      "v": 3931400
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

ticks = client.get_stock_ticks("HK", "700,9988")
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

            var ticks = client.getStockTicks("HK", "700,9988");
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

	ticks, err := client.GetStockTicks("HK", "700,9988")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock Ticks: %+v\n", ticks)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getTicks({ region: "HK", codes: ["700", "9988"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getTicks({ region: "HK", codes: ["700", "9988"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票批量实时成交 API：

```python
# 使用 MCP 工具 stockTicks
# 对应 REST API: GET /stock/ticks
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
