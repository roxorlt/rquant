<!-- source: https://docs.itick.org/zh-cn/rest-api/indices/indices-tick.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/indices/indices-tick -->

---
title: 实时成交
description: 全球主要股票指数的实时Tick数据，覆盖上证指数、沪深300、道琼斯、标普500、纳斯达克、恒生等核心指数。提供精确到毫秒的指数价格、成交量、涨跌幅及时间戳，实时反映市场最新变动。
keywords: 指数实时Tick数据, 指数Tick数据API, 股票指数实时数据, 指数高频数据接口, 实时指数行情, 上证指数实时Tick, 沪深300实时数据, 道琼斯指数Tick, 纳斯达克指数Tick, 富时AO指数Tick, 恒生指数Tick, 指数期货交易, ETF定价参考, 指数成交量数据
---

# 指数 API - 实时成交

GET /indices/tick?region={region}&code={code}

## 请求参数

| 参数名称 | 描述        | 必填 |
| -------- | ----------- | ---- |
| region   | 市场代码 GB | true |
| code     | 产品代码    | true |

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

url = "https://api.itick.org/indices/tick?region=GB&code=SPX"

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
.url("https://api.itick.org/indices/tick?region=GB&code=SPX")
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

	url := "https://api.itick.org/indices/tick?region=GB&code=SPX"

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
  path: '/indices/tick?region=GB&code=SPX',
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
curl --location 'https://api.itick.org/indices/tick?region=GB&code=SPX' \
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
    "ld": 6334.38,
    "t": 1754581840476,
    "v": 1000000
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

tick = client.get_indices_tick("GB", "SPX")
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

            var tick = client.getIndicesTick("GB", "SPX");
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

	tick, err := client.GetIndicesTick("GB", "SPX")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Indices Tick: %+v\n", tick)
}
```

### Node.js SDK

```javascript
import { IndicesClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getTick({ region: "GB", code: "SPX" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getTick({ region: "GB", code: "SPX" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用指数实时成交 API：

```python
# 使用 MCP 工具 indicesTick
# 对应 REST API: GET /indices/tick
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
