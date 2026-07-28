<!-- source: https://docs.itick.org/zh-cn/rest-api/indices/indices-depth.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/indices/indices-depth -->

---
title: 实时盘口
description: 全球主要股票指数的实时盘口深度数据，覆盖沪深300、上证指数、深证成指、标普500、道琼斯、纳斯达克、恒生等核心指数的多档买卖价格与挂单量。完整展示指数期货及相关衍生品的市场深度，实时反映买卖力量对比。
keywords: 指数实时盘口API, 指数买卖盘口, 指数期货盘口数据, 指数期货交易, 大宗交易, 做市商策略, 指数深度数据接口, 指数市场深度
---

# 指数 API - 实时盘口

GET /indices/depth?region={region}&code={code}

## 请求参数

| 参数名称 | 描述         | 必填 |
| -------- | ------------ | ---- |
| region   | 市场代码 GB  | true |
| code     | 产品代码 SPX | true |

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

url = "https://api.itick.org/indices/depth?region=GB&code=SPX"

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
.url("https://api.itick.org/indices/depth?region=GB&code=SPX")
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

	url := "https://api.itick.org/indices/depth?region=GB&code=SPX"

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
  path: '/indices/depth?region=GB&code=SPX',
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
curl --location 'https://api.itick.org/indices/depth?region=GB&code=SPX' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
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
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

depth = client.get_indices_depth("GB", "SPX")
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

            var depth = client.getIndicesDepth("GB", "SPX");
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

	depth, err := client.GetIndicesDepth("GB", "SPX")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Indices Depth: %+v\n", depth)
}
```

### Node.js SDK

```javascript
import { IndicesClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getDepth({ region: "GB", code: "SPX" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getDepth({ region: "GB", code: "SPX" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用指数实时盘口 API：

```python
# 使用 MCP 工具 indicesDepth
# 对应 REST API: GET /indices/depth
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
