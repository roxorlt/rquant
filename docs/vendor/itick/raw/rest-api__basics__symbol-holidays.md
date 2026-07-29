<!-- source: https://docs.itick.org/zh-cn/rest-api/basics/symbol-holidays.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: rest-api/basics/symbol-holidays -->

---
title: 市场假期API
description: 提供准确、及时的全球金融市场假期数据API接口，覆盖A股、港股、美股等主要交易所。数据来源可靠、更新及时，通过RESTful接口轻松集成，免费试用。
keywords: 市场假期, 股票假期接口, holidays, 假期接口
---

# 市场假期API

GET /symbol/v2/holidays

## 响应参数

| 参数名称 | 参数类型 | 描述 |
|---------|---------|------|
| c | string | 市场代码 |
| r | string | 市场国家名称 |
| tz | string | 市场时区 |
| et | string | 日内交易时间 |
| v | string | 年内假期日期 |
| ey | string | 年份 |

## 代码示例

```python
import requests

url = "https://api.itick.org/symbol/v2/holidays"

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
.url("https://api.itick.org/symbol/v2/holidays")
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

	url := "https://api.itick.org/symbol/v2/holidays"

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
  path: '/symbol/v2/holidays?{queryParameters}',
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
curl --location 'https://api.itick.org/symbol/v2/holidays?{queryParameters}' \
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
      "c": "AU",
      "r": "Australia",
      "v": "[\"2025-01-01\", \"2025-01-27\", \"2025-04-18\", \"2025-04-21\", \"2025-04-25\", \"2025-06-09\", \"2025-12-25\", \"2025-12-26\"]",
      "et": "09:30 - 16:00",
      "ey": "2025",
      "vr": null,
      "tz": 10
    }
  ]
}
```

## SDK示例

### Python SDK

```python
from itick.sdk import Client

# 初始化客户端
token = "your_api_token"
client = Client(token)

# 获取市场假期信息
holidays = client.get_symbol_holidays("US")
print(holidays)

# 获取港股市场假期
hk_holidays = client.get_symbol_holidays("HK")
print(hk_holidays)
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) {
        try {
            // 初始化客户端
            String token = "your_api_token";
            Client client = new Client(token);

            // 获取美股市场假期
            var holidays = client.getSymbolHolidays("US");
            System.out.println(holidays);

            // 获取港股市场假期
            var hkHolidays = client.getSymbolHolidays("HK");
            System.out.println(hkHolidays);

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
	// 初始化客户端
	token := "your_api_token"
	client := sdk.NewClient(token)

	// 获取美股市场假期
	holidays, err := client.GetSymbolHolidays("US")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("US Holidays: %+v\n", holidays)

	// 获取港股市场假期
	hkHolidays, err := client.GetSymbolHolidays("HK")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("HK Holidays: %+v\n", hkHolidays)
}
```

### Node.js SDK

```javascript
import { BaseClient } from "@itick/node-sdk";

// 初始化客户端
const token = process.env.ITICK_TOKEN;
const client = new BaseClient(token);

// 获取美股市场假期
async function getUSHolidays() {
  try {
    const response = await client.getSymbolHolidays("US");
    
    if (response.code === 0 && response.data) {
      console.log("美股假期:", response.data);
    }
  } catch (error) {
    console.error("错误:", error.message);
  }
}

// 获取港股市场假期
async function getHKHolidays() {
  try {
    const response = await client.getSymbolHolidays("HK");
    
    if (response.code === 0 && response.data) {
      console.log("港股假期:", response.data);
    }
  } catch (error) {
    console.error("错误:", error.message);
  }
}

getUSHolidays();
getHKHolidays();
```

### JavaScript Browser SDK

```javascript
import { BaseClient } from "@itick/browser-sdk";

// 初始化客户端
const token = process.env.ITICK_TOKEN;
const client = new BaseClient(token);

// 获取美股市场假期
async function getUSHolidays() {
  try {
    const response = await client.getSymbolHolidays("US");
    
    if (response.code === 0 && response.data) {
      console.log("美股假期:", response.data);
    }
  } catch (error) {
    console.error("错误:", error.message);
  }
}

// 获取港股市场假期
async function getHKHolidays() {
  try {
    const response = await client.getSymbolHolidays("HK");
    
    if (response.code === 0 && response.data) {
      console.log("港股假期:", response.data);
    }
  } catch (error) {
    console.error("错误:", error.message);
  }
}

getUSHolidays();
getHKHolidays();
```

### MCP Server

通过 MCP Server 调用市场假期 API：

```python
# 使用 MCP 工具 symbolHolidays
# 对应 REST API: GET /symbol/v2/holidays

# 在支持 MCP 的客户端中（如 Cursor、Claude Desktop）
# 可以直接调用 symbolHolidays 工具获取市场假期信息
```

详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。
