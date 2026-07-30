<!-- source: https://docs.itick.org/zh-cn/rest-api/basics/symbol-list.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/basics/symbol-list -->

---
title: 产品清单
description: 覆盖A股、美股、港股等全球主流市场股票、全球外汇、指数、期货、基金以及加密货币的实时、历史数据。
keywords: 金融产品清单API,股票列表数据接口,加密货币产品清单API,数字货币产品清单API,基金产品清单API，期货产品清单API,外汇产品清单API
---

# 产品清单

GET /symbol/list?type={type}&region={region}&code={code}

## 请求参数

| 参数名称 | 参数类型 | 描述                                                                                                                                                                   | 必填  |
| -------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| type     | enum     | 产品类别(stock , forex , indices , crypto , future , fund)                                                                                                             | true  |
| region   | string   | 市场代码 (股票包括（HK、SZ、SH、US、SG、JP、TW、IN、TH、DE、MX、MY、TR、ES、NL、GB、ID、VN），外汇（GB），指数（GB），数字币（BA,BT）、期货（US、HK、CN）、基金（US）) | true  |
| code     | string   | 产品代码                                                                                                                                                               | false |

## 响应参数

| 参数名称 | 参数类型 | 描述                                           |
| -------- | -------- | ---------------------------------------------- |
| c        | string   | 产品代码                                       |
| n        | string   | 产品名称                                       |
| t        | string   | 类型 如 stock,forex,indices,crypto,future,fund |
| e        | string   | 交易所                                         |

## 代码示例

```python
import requests

url = "https://api.itick.org/symbol/list?type=stock&region=HK&code=700"

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
.url("https://api.itick.org/symbol/list?type=stock&region=HK&code=700")
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

	url := "https://api.itick.org/symbol/list?type=stock&region=HK&code=700"

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
  path: '/symbol/list?type=stock&region=HK&code=700',
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
curl --location 'https://api.itick.org/symbol/list?type=stock&region=HK&code=700' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": "ok",
  "data": [
    {
      "c": "700",
      "n": "騰訊控股",
      "t": "stock",
      "e": "HKEX",
      "s": null,
      "l": "tencent"
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

symbols = client.get_symbol_list("stock", "HK", "700")
print(symbols)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var symbols = client.getSymbolList("stock", "HK", "700");
            System.out.println(symbols);
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

	symbols, err := client.GetSymbolList("stock", "HK", "700")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Symbols: %+v\n", symbols)
}
```

### Node.js SDK

```javascript
import { BaseClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new BaseClient(token);

const res = await client.getSymbolList({ type: "stock", region: "HK", code: "700" });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { BaseClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new BaseClient(token);

const res = await client.getSymbolList({ type: "stock", region: "HK", code: "700" });
console.log(res);
```

### MCP Server

通过 MCP Server 调用产品清单 API：

```python
# 使用 MCP 工具 symbolList
# 对应 REST API: GET /symbol/list
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
