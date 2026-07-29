<!-- source: https://docs.itick.org/zh-cn/rest-api/future/future-depth.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/future/future-depth -->

---
title: 实时盘口
description: 全品种期货合约的实时盘口深度数据，覆盖国内四大期货交易所及主流国际期货市场, 包含多档买卖价格、挂单量及订单簿实时变动。数据精确至毫秒级，覆盖国内外主流期货交易所。
keywords: 期货实时盘口, 期货市场深度, 买卖盘口深度, 挂单量分析, Level 2 数据, 美国期货, 香港期货, 期货做市商数据
---

# 期货 API - 实时盘口

GET /future/depth?region={region}&code={code}

## 请求参数

| 参数名称 | 描述                | 必填 |
| -------- | ------------------- | ---- |
| region   | 市场代码 US、HK、CN | true |
| code     | 产品代码 NQ         | true |

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

url = "https://api.itick.org/future/depth?region=US&code=NQ"

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
.url("https://api.itick.org/future/depth?region=US&code=NQ")
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

	url := "https://api.itick.org/future/depth?region=US&code=NQ"

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
  path: '/future/depth?region=US&code=NQ',
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
curl --location 'https://api.itick.org/future/depth?region=US&code=NQ' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
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
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

depth = client.get_future_depth("US", "NQ")
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

            var depth = client.getFutureDepth("US", "NQ");
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

	depth, err := client.GetFutureDepth("US", "NQ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Future Depth: %+v\n", depth)
}
```

### Node.js SDK

```javascript
import { FutureClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getDepth({ region: "US", code: "NQ" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FutureClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getDepth({ region: "US", code: "NQ" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用期货实时盘口 API：

```python
# 使用 MCP 工具 futureDepth
# 对应 REST API: GET /future/depth
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
