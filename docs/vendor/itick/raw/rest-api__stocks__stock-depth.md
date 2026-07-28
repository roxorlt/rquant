<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-depth.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-depth -->

---
title: 实时盘口
description: 获取股票的实时盘口深度数据，提供完整的买卖五档或十档行情，包含各价位挂单量及累计委托量。数据实时刷新，毫秒级延迟，精确反映市场买卖力量对比。
keywords: Level-2行情, 实时盘口API, 十档行情, 买卖盘口数据, 买卖五档行情, 深度数据接口, 高频盘口数据, 盘口快照, 高频交易数据, 港股盘口API, 交易所深度行情
---

# 股票 API - 实时盘口

GET /stock/depth?region={region}&code={code}

## 请求参数

| 参数名称 | 描述                                                                                    | 必填 |
| -------- | --------------------------------------------------------------------------------------- | ---- |
| region   | 市场代码 HK、SZ、SH、US、SG、JP、TW、IN、TH、DE、MX、MY、TR、ES、NL、GB、ID、VN、KR、IT | true |
| exchange | 上市交易所（可选）。相同市场存在多个股票产品时，可通过 exchange 区分查询；为空时返回主要上市交易所信息。exchange 字典可参考接口 [产品清单](/zh-cn/rest-api/basics/symbol-list) 返回的股票清单 | false |
| code     | 产品代码 700                                                                            | true |

## 响应参数

| 响应参数 | 参数类型      | 描述     |
| -------- | ------------- | -------- |
| s        | string        | 产品代码 |
| b        | array(object) | 买盘     |
| a        | array(object) | 卖盘     |
| po       | integer       | 档位     |
| p        | number        | 价格     |
| v        | number        | 挂单量   |
| o        | number        | 订单数量 |

## 代码示例

```python
import requests

url = "https://api.itick.org/stock/depth?region=HK&code=700"

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
.url("https://api.itick.org/stock/depth?region=HK&code=700")
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

	url := "https://api.itick.org/stock/depth?region=HK&code=700"

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
  path: '/stock/depth?region=HK&code=700',
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
curl --location 'https://api.itick.org/stock/depth?region=HK&code=700' \
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
    "a": [
      {
        "po": 1,
        "p": 567,
        "v": 13400,
        "o": 3
      },
      {
        "po": 2,
        "p": 567.5,
        "v": 170200,
        "o": 52
      },
      {
        "po": 3,
        "p": 568,
        "v": 268400,
        "o": 217
      },
      {
        "po": 4,
        "p": 568.5,
        "v": 126000,
        "o": 72
      },
      {
        "po": 5,
        "p": 569,
        "v": 132200,
        "o": 133
      },
      {
        "po": 6,
        "p": 569.5,
        "v": 185800,
        "o": 108
      },
      {
        "po": 7,
        "p": 570,
        "v": 423200,
        "o": 706
      },
      {
        "po": 8,
        "p": 570.5,
        "v": 108500,
        "o": 58
      },
      {
        "po": 9,
        "p": 571,
        "v": 141400,
        "o": 221
      },
      {
        "po": 10,
        "p": 571.5,
        "v": 83600,
        "o": 90
      }
    ],
    "b": [
      {
        "po": 1,
        "p": 566.5,
        "v": 24700,
        "o": 5
      },
      {
        "po": 2,
        "p": 566,
        "v": 27500,
        "o": 7
      },
      {
        "po": 3,
        "p": 565.5,
        "v": 35000,
        "o": 17
      },
      {
        "po": 4,
        "p": 565,
        "v": 177200,
        "o": 80
      },
      {
        "po": 5,
        "p": 564.5,
        "v": 42800,
        "o": 30
      },
      {
        "po": 6,
        "p": 564,
        "v": 43000,
        "o": 53
      },
      {
        "po": 7,
        "p": 563.5,
        "v": 82600,
        "o": 34
      },
      {
        "po": 8,
        "p": 563,
        "v": 103900,
        "o": 78
      },
      {
        "po": 9,
        "p": 562.5,
        "v": 58700,
        "o": 31
      },
      {
        "po": 10,
        "p": 562,
        "v": 36900,
        "o": 92
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

depth = client.get_stock_depth("HK", "700")
print(depth)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var depth = client.getStockDepth("HK", "700");
            System.out.println(depth);
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

	depth, err := client.GetStockDepth("HK", "700")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock Depth: %+v\n", depth)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getDepth({ region: "HK", code: "700" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getDepth({ region: "HK", code: "700" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票实时盘口 API：

```python
# 使用 MCP 工具 stockDepth
# 对应 REST API: GET /stock/depth
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
