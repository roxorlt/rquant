<!-- source: https://docs.itick.org/zh-cn/rest-api/stocks/stock-depths.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/stocks/stock-depths -->

---
title: 批量实时盘口
description: 批量获取多个股票的实时盘口深度数据。提供完整的买卖盘口列表，包含多档价位与挂单量，毫秒级实时更新。为您的算法交易、做市商系统、大宗交易决策提供专业的批量数据解决方案。
keywords: 股票批量盘口API, 多股并发深度行情, 市场深度数据, L2盘口批量获取, 十档买卖盘数据, 实时挂单量, 大宗交易决策, 做市商系统, 高效率盘口API, 量化盘口API
---

# 股票 API - 批量实时盘口

GET /stock/depths?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                                                                    | 必填 |
| -------- | --------------------------------------------------------------------------------------- | ---- |
| region   | 市场代码 HK、SZ、SH、US、SG、JP、TW、IN、TH、DE、MX、MY、TR、ES、NL、GB、ID、VN、IT | true |
| codes    | 货币代码，多个用英文逗号隔开，如： 700,9988                                             | true |
| exchange | 上市交易所（可选）。相同市场存在多个股票产品时，可通过 exchange 区分查询；为空时返回主要上市交易所信息。exchange 字典可参考接口 [产品清单](/zh-cn/rest-api/basics/symbol-list) 返回的股票清单 | false |

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

url = "https://api.itick.org/stock/depths?region=HK&codes=700,9988"

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
.url("https://api.itick.org/stock/depths?region=HK&codes=700,9988")
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

	url := "https://api.itick.org/stock/depths?region=HK&codes=700,9988"

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
  path: '/stock/depths?region=HK&codes=700,9988',
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
curl --location 'https://api.itick.org/stock/depths?region=HK&codes=700,9988' \
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
      "a": [
        {
          "po": 1,
          "p": 568.5,
          "v": 134900,
          "o": 3
        },
        {
          "po": 2,
          "p": 569,
          "v": 201900,
          "o": 36
        },
        {
          "po": 3,
          "p": 569.5,
          "v": 178900,
          "o": 119
        },
        {
          "po": 4,
          "p": 570,
          "v": 1087000,
          "o": 2405
        },
        {
          "po": 5,
          "p": 570.5,
          "v": 254300,
          "o": 181
        },
        {
          "po": 6,
          "p": 571,
          "v": 260800,
          "o": 388
        },
        {
          "po": 7,
          "p": 571.5,
          "v": 93400,
          "o": 117
        },
        {
          "po": 8,
          "p": 572,
          "v": 264900,
          "o": 589
        },
        {
          "po": 9,
          "p": 572.5,
          "v": 90800,
          "o": 85
        },
        {
          "po": 10,
          "p": 573,
          "v": 206800,
          "o": 284
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 568,
          "v": 75000,
          "o": 49
        },
        {
          "po": 2,
          "p": 567.5,
          "v": 160800,
          "o": 27
        },
        {
          "po": 3,
          "p": 567,
          "v": 140800,
          "o": 47
        },
        {
          "po": 4,
          "p": 566.5,
          "v": 130900,
          "o": 30
        },
        {
          "po": 5,
          "p": 566,
          "v": 183000,
          "o": 50
        },
        {
          "po": 6,
          "p": 565.5,
          "v": 62300,
          "o": 34
        },
        {
          "po": 7,
          "p": 565,
          "v": 98600,
          "o": 104
        },
        {
          "po": 8,
          "p": 564.5,
          "v": 72900,
          "o": 31
        },
        {
          "po": 9,
          "p": 564,
          "v": 38800,
          "o": 52
        },
        {
          "po": 10,
          "p": 563.5,
          "v": 31800,
          "o": 23
        }
      ]
    },
    "9988": {
      "s": "9988",
      "a": [
        {
          "po": 1,
          "p": 116.8,
          "v": 254700,
          "o": 14
        },
        {
          "po": 2,
          "p": 116.9,
          "v": 52100,
          "o": 13
        },
        {
          "po": 3,
          "p": 117,
          "v": 785600,
          "o": 57
        },
        {
          "po": 4,
          "p": 117.1,
          "v": 482000,
          "o": 23
        },
        {
          "po": 5,
          "p": 117.2,
          "v": 180300,
          "o": 20
        },
        {
          "po": 6,
          "p": 117.3,
          "v": 155100,
          "o": 16
        },
        {
          "po": 7,
          "p": 117.4,
          "v": 279800,
          "o": 22
        },
        {
          "po": 8,
          "p": 117.5,
          "v": 416000,
          "o": 76
        },
        {
          "po": 9,
          "p": 117.6,
          "v": 285500,
          "o": 43
        },
        {
          "po": 10,
          "p": 117.7,
          "v": 168900,
          "o": 36
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 116.7,
          "v": 98500,
          "o": 48
        },
        {
          "po": 2,
          "p": 116.6,
          "v": 631300,
          "o": 110
        },
        {
          "po": 3,
          "p": 116.5,
          "v": 1091600,
          "o": 206
        },
        {
          "po": 4,
          "p": 116.4,
          "v": 681000,
          "o": 53
        },
        {
          "po": 5,
          "p": 116.3,
          "v": 755400,
          "o": 60
        },
        {
          "po": 6,
          "p": 116.2,
          "v": 354600,
          "o": 58
        },
        {
          "po": 7,
          "p": 116.1,
          "v": 322600,
          "o": 63
        },
        {
          "po": 8,
          "p": 116,
          "v": 2012300,
          "o": 290
        },
        {
          "po": 9,
          "p": 115.9,
          "v": 361800,
          "o": 33
        },
        {
          "po": 10,
          "p": 115.8,
          "v": 343100,
          "o": 52
        }
      ]
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

depths = client.get_stock_depths("HK", "700,9988")
print(depths)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var depths = client.getStockDepths("HK", "700,9988");
            System.out.println(depths);
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

	depths, err := client.GetStockDepths("HK", "700,9988")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Stock Depths: %+v\n", depths)
}
```

### Node.js SDK

```javascript
import { StockClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getDepths({ region: "HK", codes: ["700", "9988"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { StockClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new StockClient(token);

const res = await client.getDepths({ region: "HK", codes: ["700", "9988"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用股票批量实时盘口 API：

```python
# 使用 MCP 工具 stockDepths
# 对应 REST API: GET /stock/depths
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
