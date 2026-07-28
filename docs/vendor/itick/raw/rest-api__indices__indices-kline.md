<!-- source: https://docs.itick.org/zh-cn/rest-api/indices/indices-kline.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/indices/indices-kline -->

---
title: 历史K线查询
description: 全球主要股票指数的完整K线数据，提供沪深300、上证指数、深证成指、创业板指、标普500、纳斯达克、恒生等指数的多周期OHLC价格序列。支持从分钟线到月线的多时间周期，包含开盘价、最高价、最低价、收盘价及成交量。
keywords: 指数K线数据API, 股票指数K线查询, 指数OHLC数据接口, 指数历史行情数据, 沪深300 K线数据, 指数成交量历史, 标普500历史价格, 指数开盘收盘价
---

# 指数 API - 历史K线查询

GET /indices/kline?region={region}&code={code}&kType={kType}&limit={limit}&et={et}

## 请求参数

| 参数名称 | 描述                                                                                                 | 必填  |
| -------- | ---------------------------------------------------------------------------------------------------- | ----- |
| region   | 市场代码 GB                                                                                          | true  |
| code     | 产品代码 SPX                                                                                         | true  |
| kType    | K线类型(1 分钟K，2 5分钟K，3 15分钟K，4 30分钟K，5 1小时K，6 2小时K，7 4小时K，8 日K，9 周K，10 月K) | true  |
| limit    | K线数量                                                                                              | true  |
| et       | 截止时间戳 (为空时默认为当前时间戳)                                                                  | false |

## 响应参数

| 响应参数 | 参数类型 | 描述     |
| -------- | -------- | -------- |
| t        | number   | 时间戳   |
| o        | number   | 开盘价   |
| h        | number   | 最高价   |
| l        | number   | 最低价   |
| c        | number   | 收盘价   |
| v        | number   | 成交数量 |
| tu       | number   | 成交额   |

## 代码示例

```python
import requests

url = "https://api.itick.org/indices/kline?region=GB&code=SPX&kType=2&limit=10"

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
.url("https://api.itick.org/indices/kline?region=GB&code=SPX&kType=2&limit=10")
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

	url := "https://api.itick.org/indices/kline?region=GB&code=SPX&kType=2&limit=10"

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
  path: '/indices/kline?region=GB&code=SPX&kType=2&limit=10',
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
curl --location 'https://api.itick.org/indices/kline?region=GB&code=SPX&kType=2&limit=10' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": [
    {
      "tu": 385612920000,
      "c": 5842.62,
      "t": 1741208580000,
      "v": 66000000,
      "h": 5842.62,
      "l": 5842.62,
      "o": 5842.62
    }
  ]
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

kline = client.get_indices_kline("GB", "SPX", 2, 10)
print(kline)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var kline = client.getIndicesKline("GB", "SPX", 2, 10, null);
            System.out.println(kline);
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

	kline, err := client.GetIndicesKline("GB", "SPX", 2, 10, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Indices Kline: %+v\n", kline)
}
```

### Node.js SDK

```javascript
import { IndicesClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getKline({
  region: "GB",
  code: "SPX",
  interval: "5m",
  limit: 10,
});
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { IndicesClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new IndicesClient(token);

const res = await client.getKline({
  region: "GB",
  code: "SPX",
  interval: "5m",
  limit: 10,
});
console.log(res);
```

### MCP Server

通过 MCP Server 调用指数历史K线 API：

```python
# 使用 MCP 工具 indicesKline
# 对应 REST API: GET /indices/kline
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
