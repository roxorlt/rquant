<!-- source: https://docs.itick.org/zh-cn/rest-api/fund/fund-tick.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/fund/fund-tick -->

---
title: 实时成交
description: 提供全面的基金数据API。对于场内基金，提供ETF、LOF等品种的实时Tick级成交与盘口数据；对于场外基金，提供准确的盘中实时净值估算（IOPV）与收盘后官方净值。数据覆盖中国基金、美国基金、香港基金等主流市场。
keywords: 基金实时Tick数据, ETF逐笔成交, LOF交易数据, 基金行情API, 场内基金Tick, 基金IOPV数据, ETF套利数据, 基金盘口快照
---

# 基金 API - 实时成交

GET /fund/tick?region={region}&code={code}

## 请求参数

| 参数名称 | 描述        | 必填 |
| -------- | ----------- | ---- |
| region   | 市场代码 US | true |
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

url = "https://api.itick.org/fund/tick?region=US&code=QQQ"

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
.url("https://api.itick.org/fund/tick?region=US&code=QQQ")
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

	url := "https://api.itick.org/fund/tick?region=US&code=QQQ"

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
  path: '/fund/tick?region=US&code=QQQ',
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
curl --location 'https://api.itick.org/fund/tick?region=US&code=QQQ' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "s": "QQQ",
    "ld": 553.88,
    "t": 1754092799000,
    "v": 11888
  }
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

tick = client.get_fund_tick("US", "QQQ")
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

            var tick = client.getFundTick("US", "QQQ");
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

	tick, err := client.GetFundTick("US", "QQQ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Fund Tick: %+v\n", tick)
}
```

### Node.js SDK

```javascript
import { FundClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getTick({ region: "US", code: "QQQ" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FundClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getTick({ region: "US", code: "QQQ" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用基金实时成交 API：

```python
# 使用 MCP 工具 fundTick
# 对应 REST API: GET /fund/tick
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
