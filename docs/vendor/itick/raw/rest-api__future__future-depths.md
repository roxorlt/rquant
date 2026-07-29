<!-- source: https://docs.itick.org/zh-cn/rest-api/future/future-depths.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/future/future-depths -->

---
title: 批量实时盘口
description: 批量获取多个期货合约的实时订单簿深度数据。提供完整的买卖盘口列表，包含多档价位与挂单量，毫秒级实时更新。
keywords: 期货批量盘口API, 批量订单簿数据, 期货市场深度, Level 2 数据, 多合约盘口, 买卖盘口深度, 实时挂单量, 大宗交易决策, 做市商系统
---

# 期货 API - 批量实时盘口

GET /future/depths?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                    | 必填 |
| -------- | --------------------------------------- | ---- |
| region   | 市场代码 US、HK、CN                     | true |
| codes    | 货币代码，多个用英文逗号隔开，如：NQ,ES | true |

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

url = "https://api.itick.org/future/depths?region=US&codes=NQ,ES"

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
.url("https://api.itick.org/future/depths?region=US&codes=NQ,ES")
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

	url := "https://api.itick.org/future/depths?region=US&codes=NQ,ES"

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
  path: '/future/depths?region=US&codes=NQ,ES',
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
curl --location 'https://api.itick.org/future/depths?region=US&codes=NQ,ES' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "NQ": {
      "s": "NQ",
      "a": [
        {
          "po": 1,
          "p": 22949,
          "v": 1,
          "o": 1
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 22948.25,
          "v": 3,
          "o": 1
        }
      ]
    },
    "ES": {
      "s": "ES",
      "a": [
        {
          "po": 1,
          "v": 0,
          "o": 0
        }
      ],
      "b": [
        {
          "po": 1,
          "v": 0,
          "o": 0
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

depths = client.get_future_depths("US", "NQ,ES")
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

            var depths = client.getFutureDepths("US", "NQ,ES");
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

	depths, err := client.GetFutureDepths("US", "NQ,ES")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Future Depths: %+v\n", depths)
}
```

### Node.js SDK

```javascript
import { FutureClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getDepths({ region: "US", codes: ["NQ", "ES"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FutureClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getDepths({ region: "US", codes: ["NQ", "ES"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用期货批量实时盘口 API：

```python
# 使用 MCP 工具 futureDepths
# 对应 REST API: GET /future/depths
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
