<!-- source: https://docs.itick.org/zh-cn/rest-api/indices/indices-ticks.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/indices/indices-ticks -->

---
title: 批量实时成交
description: 批量获取全球主要股票指数的实时逐笔成交数据，覆盖沪深300、上证指数、深证成指、标普500、纳斯达克、恒生等核心指数。提供精确至毫秒的时间戳、成交价格、成交量及买卖方向，完整记录市场每笔成交细节。
keywords: 指数批量Tick数据, 指数实时成交API, 批量指数逐笔数据, 指数高频数据接口, 指数期货套利, 指数成交量分析, 沪深300逐笔成交
---

# 指数 API - 批量实时成交

GET /indices/ticks?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                            | 必填 |
| -------- | ----------------------------------------------- | ---- |
| region   | 市场代码 GB                                     | true |
| codes    | 产品代码,多个用英文逗号隔开，如： SPX,DJI       | true |

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

url = "https://api.itick.org/indices/ticks?region=GB&codes=SPX,DJI"

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
.url("https://api.itick.org/indices/ticks?region=GB&codes=SPX,DJI")
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

	url := "https://api.itick.org/indices/ticks?region=GB&codes=SPX,DJI"

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
  path: '/indices/ticks?region=GB&codes=SPX,DJI',
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
curl --location 'https://api.itick.org/indices/ticks?region=GB&codes=SPX,DJI' \
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
      "ld": 6338.25,
      "t": 1754581081000,
      "v": 1000000
    },
    "DJI": {
      "s": "DJI",
      "ld": 43874.46,
      "t": 1754581081000,
      "v": 100000
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

ticks = client.get_indices_ticks("GB", "SPX,DJI")
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

            var ticks = client.getIndicesTicks("GB", "SPX,DJI");
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

	ticks, err := client.GetIndicesTicks("GB", "SPX,DJI")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Indices Ticks: %+v\n", ticks)
}
```

### Node.js SDK

```javascript
import { IndicesClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getTicks({ region: "GB", codes: ["SPX", "DJI"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getTicks({ region: "GB", codes: ["SPX", "DJI"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用指数批量实时成交 API：

```python
# 使用 MCP 工具 indicesTicks
# 对应 REST API: GET /indices/ticks
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
