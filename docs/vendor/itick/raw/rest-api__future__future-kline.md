<!-- source: https://docs.itick.org/zh-cn/rest-api/future/future-kline.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/future/future-kline -->

---
title: 历史K线查询
description: 全品种期货合约的完整K线数据，覆盖国内四大期货交易所及主流国际期货市场, 含盖商品期货、金融期货等各类品种。包含开盘价、最高价、最低价、收盘价、成交量及持仓量等完整指标，支持从分钟线到月线的多周期查询。数据经过复权处理，确保连续性。
keywords: 期货K线数据, 期货OHLC接口, 期货主力合约, 期货历史行情, K线数据API, 美国期货, 香港期货, 商品期货K线, 金融期货行情
---

# 期货 API - 历史K线查询

GET /future/kline?region={region}&code={code}&kType={kType}&limit={limit}&et={et}

## 请求参数

| 参数名称 | 描述                                                                                                 | 必填  |
| -------- | ---------------------------------------------------------------------------------------------------- | ----- |
| region   | 市场代码 US、HK、CN                                                                                  | true  |
| code     | 产品代码 NQ                                                                                          | true  |
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

url = "https://api.itick.org/future/kline?region=US&code=NQ&kType=2&limit=10"

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
.url("https://api.itick.org/future/kline?region=US&code=NQ&kType=2&limit=10")
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

	url := "https://api.itick.org/future/kline?region=US&code=NQ&kType=2&limit=10"

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
  path: '/future/kline?region=US&code=NQ&kType=2&limit=10',
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
curl --location 'https://api.itick.org/future/kline?region=US&code=NQ&kType=2&limit=10' \
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
      "tu": 31835798.75,
      "c": 23535,
      "t": 1754656800000,
      "v": 1353,
      "h": 23539.25,
      "l": 23518.5,
      "o": 23527.5
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

kline = client.get_future_kline("US", "NQ", 2, 10)
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

            var kline = client.getFutureKline("US", "NQ", 2, 10, null);
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

	kline, err := client.GetFutureKline("US", "NQ", 2, 10, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Future Kline: %+v\n", kline)
}
```

### Node.js SDK

```javascript
import { FutureClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getKline({
  region: "US",
  code: "NQ",
  interval: "5m",
  limit: 10,
});
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FutureClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FutureClient(token);

const res = await client.getKline({
  region: "US",
  code: "NQ",
  interval: "5m",
  limit: 10,
});
console.log(res);
```

### MCP Server

通过 MCP Server 调用期货历史K线 API：

```python
# 使用 MCP 工具 futureKline
# 对应 REST API: GET /future/kline
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
