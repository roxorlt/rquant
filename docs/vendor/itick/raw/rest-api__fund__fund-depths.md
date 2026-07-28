<!-- source: https://docs.itick.org/zh-cn/rest-api/fund/fund-depths.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/fund/fund-depths -->

---
title: 批量实时盘口
description: 提供场内ETF、LOF等基金的批量实时盘口数据流，完整呈现多只基金的多档买卖价格、挂单量及订单簿深度。数据持续实时更新，毫秒级延迟确保市场深度精准同步。
keywords: 基金批量实时盘口API, ETF深度数据, LOF订单簿, 基金市场深度, 基金买卖盘口, 实时挂单量, 基金Level-2行情, 场内基金盘口, 大宗交易执行
---

# 基金 API - 批量实时盘口

GET /fund/depths?region={region}&codes={codes}

## 请求参数

| 参数名称 | 描述                                      | 必填 |
| -------- | ----------------------------------------- | ---- |
| region   | 市场代码 US                               | true |
| codes    | 货币代码，多个用英文逗号隔开，如：QQQ,IEF | true |

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

url = "https://api.itick.org/fund/depths?region=US&codes=QQQ,IEF"

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
.url("https://api.itick.org/fund/depths?region=US&codes=QQQ,IEF")
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

	url := "https://api.itick.org/fund/depths?region=US&codes=QQQ,IEF"

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
  path: '/fund/depths?region=US&codes=QQQ,IEF',
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
curl --location 'https://api.itick.org/fund/depths?region=US&codes=QQQ,IEF' \
--header 'accept: application/json' \
--header 'token: {token}'
```

## 响应结果

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "QQQ": {
      "s": "QQQ",
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
    },
    "IEF": {
      "s": "IEF",
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
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

depths = client.get_fund_depths("US", "QQQ,IEF")
print(depths)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            String token = "your_api_token";
            Client client = new Client(token);

            var depths = client.getFundDepths("US", "QQQ,IEF");
            System.out.println(depths);
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

	depths, err := client.GetFundDepths("US", "QQQ,IEF")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Fund Depths: %+v\n", depths)
}
```

### Node.js SDK

```javascript
import { FundClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getDepths({ region: "US", codes: ["QQQ", "IEF"] });
console.log(res);
```

### JavaScript Browser SDK

```javascript
import { FundClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new FundClient(token);

const res = await client.getDepths({ region: "US", codes: ["QQQ", "IEF"] });
console.log(res);
```

### MCP Server

通过 MCP Server 调用基金批量实时盘口 API：

```python
# 使用 MCP 工具 fundDepths
# 对应 REST API: GET /fund/depths
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
