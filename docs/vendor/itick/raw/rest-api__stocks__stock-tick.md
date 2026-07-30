<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-tick.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-tick -->

---
title: 实时成交
description: 提供低延迟、高可用的股票Tick级数据API，包含完整的逐笔成交、买卖盘口（十档行情）、实时买卖挂单及Level-2深度数据。数据源直接对接交易所，覆盖A股、美股、港股等全球主流市场。
keywords: Tick数据API, 逐笔成交数据, Level-2行情, 股票盘口接口, 高频交易数据, 实时数据推送, 十档行情API, 低延迟行情, 量化交易接口, 历史Tick数据, 股票行情接口
---

# 股票 API - 实时成交

GET /stock/tick?region={region}&code={code}

## 请求参数

| 参数名称 | 描述                                                                                    | 必填 |
| -------- | --------------------------------------------------------------------------------------- | ---- |
| region   | 市场代码 HK、SZ、SH、US、SG、JP、TW、IN、TH、DE、MX、MY、TR、ES、NL、GB、ID、VN、IT | true |
| exchange | 上市交易所（可选）。相同市场存在多个股票产品时，可通过 exchange 区分查询；为空时返回主要上市交易所信息。exchange 字典可参考接口 [产品清单](/zh-cn/rest-api/basics/symbol-list) 返回的股票清单 | false |
| code     | 产品代码                                                                                | true |

## 响应参数

| 参数名称 | 参数类型 | 描述     |
| -------- | -------- | -------- |
| s        | string   | 产品代码 |
| ld       | number   | 最新价   |
| t        | number   | 时间戳   |
| v        | number   | 成交数量 |
| te       | number   | 交易时段 0:常规交易 1:盘前交易 2:盘后交易 |

## 代码示例

```python
import requests

url = "https://api.itick.org/stock/tick?region=HK&code=700"

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
.url("https://api.itick.org/stock/tick?region=HK&code=700")
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

	url := "https://api.itick.org/stock/tick?region=HK&code=700"

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
  path: '/stock/tick?region=HK&code=700',
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
curl --location 'https://api.itick.org/stock/tick?region=HK&code=700' \
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
    "ld": 567,
    "t": 1754554087000,
    "v": 1134500,
		"te": 0
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

tick = client.get_stock_tick("HK", "700")
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

            var tick = client.getStockTick("HK", "700");
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

	tick, err := client.GetStockTick("HK", "700")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock Tick: %+v\n", tick)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getTick({ region: "HK", code: "700" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getTick({ region: "HK", code: "700" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票实时成交 API：

```python
# 使用 MCP 工具 stockTick
# 对应 REST API: GET /stock/tick
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
