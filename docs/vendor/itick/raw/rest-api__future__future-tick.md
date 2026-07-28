<!-- source: https://docs.itick.org/zh-cn/rest-api/future/future-tick.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/future/future-tick -->

---
title: 实时成交
description: 全品种期货合约的实时Tick数据流，包含逐笔成交明细与实时盘口快照。数据精确至毫秒级，并通过WebSocket API持续推送，确保低延迟。覆盖国内四大期货交易所及主流国际期货市场。
keywords: 期货实时Tick数据, 期货逐笔成交, Tick数据API, 低延迟期货API, 期货高频交易, 微秒级期货行情, 期货交易所直连, 期货Tick数据流, 期货算法交易, 期货做市商系统, 极速期货行情, 期货套利数据
---

# 期货 API - 实时成交

GET /future/tick?region={region}&code={code}

## 请求参数

| 参数名称 | 描述                | 必填 |
| -------- | ------------------- | ---- |
| region   | 市场代码 US、HK、CN | true |
| code     | 产品代码            | true |

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

url = "https://api.itick.org/future/tick?region=HK&code=NQ"

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
.url("https://api.itick.org/future/tick?region=HK&code=NQ")
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

	url := "https://api.itick.org/future/tick?region=HK&code=NQ"

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
  path: '/future/tick?region=HK&code=NQ',
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
curl --location 'https://api.itick.org/future/tick?region=HK&code=NQ' \
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
    "ld": 22948.5,
    "t": 1754062000728,
    "v": 17
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

tick = client.get_future_tick("HK", "NQ")
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

            var tick = client.getFutureTick("HK", "NQ");
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

	tick, err := client.GetFutureTick("HK", "NQ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Future Tick: %+v\n", tick)
}
```

### Node.js SDK

```javascript
import { FutureClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getTick({ region: "HK", code: "NQ" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FutureClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getTick({ region: "HK", code: "NQ" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用期货实时成交 API：

```python
# 使用 MCP 工具 futureTick
# 对应 REST API: GET /future/tick
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
