<!-- source: https://docs.itick.org/zh-cn/rest-api/indices/indices-depths.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/indices/indices-depths -->

---
title: 批量实时盘口
description: 批量获取全球主要股票指数的实时盘口深度数据，覆盖沪深300、上证指数、深证成指、标普500、纳斯达克、恒生等核心指数。提供完整的买卖盘口列表，包含多档价位与挂单量，毫秒级实时更新。
keywords: 指数批量盘口API, 指数深度数据, 多指数订单簿, 市场深度数据, 指数买卖档位数据, 实时挂单量, 做市商系统, 大宗交易决策
---

# 指数 API - 批量实时盘口

GET /indices/depths?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                      | 必填 |
| -------- | ----------------------------------------- | ---- |
| region   | 市场代码 GB                               | true |
| codes    | 货币代码，多个用英文逗号隔开，如：SPX,DJI | true |

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

url = "https://api.itick.org/indices/depths?region=GB&codes=SPX,DJI"

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
.url("https://api.itick.org/indices/depths?region=GB&codes=SPX,DJI")
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

	url := "https://api.itick.org/indices/depths?region=GB&codes=SPX,DJI"

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
  path: '/indices/depths?region=GB&codes=SPX,DJI',
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
curl --location 'https://api.itick.org/indices/depths?region=GB&codes=SPX,DJI' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "SPX": {
      "s": "SPX",
      "a": [
        {
          "po": 1,
          "p": 0,
          "v": 0,
          "o": 1
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 0,
          "v": 0,
          "o": 1
        }
      ]
    },
    "DJI": {
      "s": "DJI",
      "a": [
        {
          "po": 1,
          "p": 0,
          "v": 0,
          "o": 1
        }
      ],
      "b": [
        {
          "po": 1,
          "p": 0,
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

depths = client.get_indices_depths("GB", "SPX,DJI")
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

            var depths = client.getIndicesDepths("GB", "SPX,DJI");
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

	depths, err := client.GetIndicesDepths("GB", "SPX,DJI")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Indices Depths: %+v\n", depths)
}
```

### Node.js SDK

```javascript
import { IndicesClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getDepths({ region: "GB", codes: ["SPX", "DJI"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getDepths({ region: "GB", codes: ["SPX", "DJI"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用指数批量实时盘口 API：

```python
# 使用 MCP 工具 indicesDepths
# 对应 REST API: GET /indices/depths
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
