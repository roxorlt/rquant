<!-- source: https://docs.itick.org/zh-cn/rest-api/forex/forex-depths.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/forex/forex-depths -->

---
title: 批量实时盘口
description: 批量获取多个外汇货币对的实时订单簿深度数据，覆盖EUR、GBP、JPY、CHF等主要货币对。提供各货币对完整的买卖盘口列表，包含多档价位与挂单量，毫秒级实时更新。
keywords: 外汇批量盘口API, 外汇批量深度数据, 多货币对盘口接口,外汇市场深度批量, 外汇做市商系统, 高频交易数据源, 外汇买卖盘口批量
---

# 外汇 API - 批量实时盘口

GET /forex/depths?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                               | 必填 |
| -------- | -------------------------------------------------- | ---- |
| region   | 市场代码 GB                                     | true |
| codes    | 货币代码，多个用英文逗号隔开，如： EURUSD,GBPUSD | true |

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

url = "https://api.itick.org/forex/depths?region=GB&codes=EURUSD,GBPUSD"

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
.url("https://api.itick.org/forex/depths?region=GB&codes=EURUSD,GBPUSD")
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

	url := "https://api.itick.org/forex/depths?region=GB&codes=EURUSD,GBPUSD"

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
  path: '/forex/depths?region=GB&codes=EURUSD,GBPUSD',
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
curl --location 'https://api.itick.org/forex/depths?region=GB&codes=EURUSD,GBPUSD' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "EURUSD": {
      "s": "EURUSD",
      "a": [
        {
          "po": 1,
          "p": 1.16443,
          "v": 0,
          "o": 1
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 1.16441,
          "v": 0,
          "o": 1
        }
      ]
    },
    "GBPUSD": {
      "s": "GBPUSD",
      "a": [
        {
          "po": 1,
          "p": 1.34324,
          "v": 0,
          "o": 1
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 1.3431899999999999,
          "v": 0,
          "o": 1
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

depths = client.get_forex_depths("GB", "EURUSD,GBPUSD")
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

            var depths = client.getForexDepths("GB", "EURUSD,GBPUSD");
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

	depths, err := client.GetForexDepths("GB", "EURUSD,GBPUSD")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Forex Depths: %+v\n", depths)
}
```

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getDepths({ region: "GB", codes: ["EURUSD", "GBPUSD"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const res = await client.getDepths({ region: "GB", codes: ["EURUSD", "GBPUSD"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用外汇批量实时盘口 API：

```python
# 使用 MCP 工具 forexDepths
# 对应 REST API: GET /forex/depths
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
