<!-- source: https://docs.itick.org/zh-cn/rest-api/future/future-ticks.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/future/future-ticks -->

---
title: 批量实时成交
description: 批量获取多个期货合约的实时Tick数据，覆盖国内四大期货交易所及主流国际期货市场, 含盖商品期货、金融期货等全品种合约。提供精确到毫秒级的逐笔成交记录，包含成交价格、成交量、买卖方向及时间戳，同时附带实时盘口快照。
keywords: 期货批量Tick数据, 多合约Tick接口, 期货逐笔成交, 高频数据API, 商品期货Tick数据, 美国期货, 香港期货, 期货盘口快照, 金融期货行情
---

# 期货 API - 批量实时成交

GET /future/ticks?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                   | 必填 |
| -------- | -------------------------------------- | ---- |
| region   | 市场代码 US、HK、CN                    | true |
| codes    | 产品代码,多个用英文逗号隔开，如：NQ,ES | true |

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

url = "https://api.itick.org/future/ticks?region=US&codes=NQ,ES"

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
.url("https://api.itick.org/future/ticks?region=US&codes=NQ,ES")
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

	url := "https://api.itick.org/future/ticks?region=US&codes=NQ,ES"

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
  path: '/future/ticks?region=US&codes=NQ,ES',
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
curl --location 'https://api.itick.org/future/ticks?region=US&codes=NQ,ES' \
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
      "ld": 22948.5,
      "t": 1754062000728,
      "v": 17
    },
    "ES": {
      "s": "ES",
      "ld": 65.04,
      "t": 1754689608000,
      "v": 181428
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

ticks = client.get_future_ticks("US", "NQ,ES")
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

            var ticks = client.getFutureTicks("US", "NQ,ES");
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

	ticks, err := client.GetFutureTicks("US", "NQ,ES")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Future Ticks: %+v\n", ticks)
}
```

### Node.js SDK

```javascript
import { FutureClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getTicks({ region: "US", codes: ["NQ", "ES"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FutureClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getTicks({ region: "US", codes: ["NQ", "ES"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用期货批量实时成交 API：

```python
# 使用 MCP 工具 futureTicks
# 对应 REST API: GET /future/ticks
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
