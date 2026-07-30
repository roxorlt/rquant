<!-- source: https://docs.itick.org/zh-cn/websocket/forex.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: websocket/forex -->

---
title: 外汇报价
description: 提供全球最新外汇数据的流式访问，实时推送EUR、GBP、JPY、CHF等主流货币对的实时报价、Tick成交、订单簿深度及汇率变动。数据源覆盖多家流动性提供商，毫秒级低延迟推送。
keywords: 外汇WebSocket, 外汇实时行情, WebSocket API, 外汇Tick数据, EUR/USD实时报价, 外汇实时行情推送, 货币对实时数据
---

## 外汇 WebSocket 文档

iTick Forex WebSocket API 提供全球最新外汇数据的流式访问。 您可以通过以操作形式发送指令来指定要使用的频道。当您订阅的频道中发生事件时，我们的 WebSockets 会发出事件以通知您。

我们的 WebSocket API 基于授权，授权可控制您可以连接到哪些 WebSocket 集群以及您可以访问哪些类型的数据。 您可以登录查看包含您的 API 密钥并根据您的授权进行个性化的示例。

### 第 1 步：连接

单个websocket连接最高订阅500个产品，不区分类型(types)，不同的套餐级别拥有的websocket连接不同，超过连接数权限眼制，则新的订阅将建立失败，如果需要增加连接个数，可以联系客服申请

连接到集群：

```shell
wscat -c wss://api.itick.org/forex -H "token: 2abf6c0*************************dd8a1930a2f48ba14a"
```

连接后您将收到以下消息：

```json
{
  "code": 1,
  "msg": "Connected Successfully"
}
```

### 第 2 步：验证

验证成功后，您将收到以下消息：

```json
{
  "code": 1,
  "resAc": "auth",
  "msg": "authenticated"
}
```

验证失败，会断开连接，流程终止

```json
{
  "code": 0,
  "resAc": "auth",
  "msg": "auth failed"
}
```

### 第 3 步：订阅

验证身份后，即可请求流。您可以在同一请求中请求多个流。

```json
{
  "ac": "subscribe",
  "params": "EURUSD$GB,GBPUSD$GB",
  "types": "quote"
}
```

> params：标的`symbol$region`，支持订阅多个，多个用英文逗号隔开，单个WS最大订阅数为500，超过则会被限制\
> types: 订阅的类型 `depth`：盘口、`quote`：报价、`tick`：成交、`kline`：K线（订阅1分钟参数：`kline@1`）\
> 注意:`kline@1`目前只有高级以上、股票套餐支持 

订阅成功返回内容。

```json
{
  "code": 1,
  "resAc": "subscribe",
  "msg": "subscribe Successfully"
}
```

订阅失败返回内容。如下：分别是超出套餐计划最大数量，订阅参数错误。

```json
{
  "code": 0,
  "resAc": "subscribe",
  "msg": "exceeding the maximum subscription limit"
}
```

```json
{
  "code": 0,
  "resAc": "subscribe",
  "msg": "cannot be resolved action"
}
```

### 第 4 步：响应内容

iTick.org WebSocket 客户端必须能够每秒处理许多传入消息。由于 WebSocket 协议的性质，如果客户端从服务器获取消息的速度很慢，iTick.org 的服务器必须缓冲消息，并以客户端可以接收的速度发送消息。如果客户端长时间以太慢的速度消费消息， iTick.org的服务器端缓冲区可能会变得太大。如果发生这种情况，iTick.org 将终止 WebSocket 连接。如果您经常遇到这种情况，请考虑订阅较少的符号或频道。

订阅成功后数据按照如下内容发送。

#### 成交响应内容

```json
{
  "code": 1,
  "data": {
    "s": "EURUSD",        // 标的 symbol
    "r": "GB",            // 标的 region
    "ld": 225.215,        // 最新价
    "v": 16742235,        // 成交量
    "t": 1731689407000,   // 时间戳
    "type": "tick"        // 数据类型 tick、quote、depth
  }
}
```

#### 报价响应内容

```json
{
  "code": 1,
  "data": {
    "s": "EURUSD",         // 标的 symbol
    "r": "GB",             // 标的 region
    "ld": 225.215,         // 最新价
    "o": 226.27,           // 开盘价
    "h": 226.92,           // 最高价
    "l": 224.44,           // 最低价
    "t": 1731689407000,    // 时间戳 毫秒
    "v": 16742235,         // 当前交易日内成交量
    "tu": 3774688301.452,  // 当前交易日内成交额
    "type": "quote"        // 数据类型 tick、quote、depth
  }
}
```

#### 盘口响应内容

```json
{
  "code": 1,
  "data": {
    "s": "EURUSD$GB",     // 标的 symbol
    "r": "GB",            // 标的 region
    "a": [                // 盘口 ask
      {
        "po": 1,          // 盘口档位
        "p": 3034.01,     // 盘口价格
        "v": 10.6023,     // 盘口数量
        "o": 10.6023      // 盘口委托量
      }
    ],
    "b": [                // 盘口 bid
      {
        "po": 1,          // 盘口档位
        "p": 3034,        // 盘口价格
        "v": 20.9758,     // 盘口数量
        "o": 20.9758      // 盘口委托量
      }
    ],
    "type": "depth"       // 数据类型 depth
  }
}
```

#### K线响应内容

```json
{
  "code": 1,
  "data": {
      "tu": 157513,        // 当前周期总成交额
      "c": 3059.39,        // 当前周期收盘价
      "t": 1731660060000,  // 周期时间戳 毫秒
      "v": 28,             // 当前周期总成交量
      "h": 3061.41,        // 当前周期最高价
      "l": 3055.24,        // 当前周期最低价
      "o": 3055.36,        // 当前周期开始价
      "type": "kline@1",   // K线周期
      "s": "EURUSD",       // 标的 symbol
      "r": "GB"            // 标的 region
  }
}
```

> t Kline 周期： 周期 1分钟、2五分钟、3十五分钟、4三十分钟、5一小时、8一天、9一周、10一月

### 第 5 步：保持心跳

客户端向服务器发送,如果超过1分钟没有心跳，服务会在适当的时机后断开与客户端的链接，建议至少每30秒内发送一次心跳，保持与服务端的链接

```json
{
  "ac": "ping",
  "params": "1731688569840"
}
```

服务端向客户端发送

```json
{
  "resAc": "pong",
  "data": { "params": "1731688569840" }
}
```

> ping、pong的时间戳需要保持一致

## SDK示例

### Node.js SDK

```javascript
import { ForexClient } from "@itick/node-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const socket = client.createSocket({
  subscribeData: {
    codes: ["EURUSD$GB", "GBPUSD$GB"],
    types: ["quote", "tick", "depth"],
  },
});

socket.onSocketMessage((res) => {
  console.log("收到数据:", res);
});
```

### JavaScript Browser SDK

```javascript
import { ForexClient } from "@itick/browser-sdk";

const token = process.env.ITICK_TOKEN;
const client = new ForexClient(token);

const socket = client.createSocket({
  subscribeData: {
    codes: ["EURUSD$GB", "GBPUSD$GB"],
    types: ["quote", "tick", "depth"],
  },
});

socket.onSocketMessage((res) => {
  console.log("收到数据:", res);
});
```

### Python SDK

```python
from itick.sdk import Client

token = "your_api_token"
client = Client(token)

client.connect_forex_websocket()
client.send_websocket_message('{"ac":"subscribe","params":"EURUSD$GB","types":"quote"}')
```

### Java SDK

```java
import io.itick.sdk.Client;

public class Main {
    public static void main(String[] args) throws Exception {
        String token = "your_api_token";
        Client client = new Client(token);

        client.connectForexWebSocket();
        client.sendWebSocketMessage("{\"ac\":\"subscribe\",\"params\":\"EURUSD$GB\",\"types\":\"quote\"}");
    }
}
```

### Go SDK

```go
package main

import (
	"io.github.itick/sdk"
)

func main() {
	token := "your_api_token"
	client := sdk.NewClient(token)

	_ = client.ConnectForexWebSocket()
	_ = client.SendWebSocketMessage([]byte(`{"ac":"subscribe","params":"EURUSD$GB","types":"quote"}`))
}
```

### MCP Server

MCP Server 当前仅实现 REST 工具，未实现 WebSocket 订阅能力。详细配置请参考 [MCP Server 文档](https://docs.itick.org/zh-cn/sdk/mcp-server)。

## python 示例代码

```python
import websocket
import json
import threading
import time

# WebSocket 连接地址和 token
WS_URL = "wss://api.itick.org/forex"
API_TOKEN = "your_token"

def on_message(ws, message):
    """处理接收到的消息"""
    print("Received message:", message)
    data = json.loads(message)

    # 处理连接成功的消息
    if data.get("code") == 1 and data.get("msg") == "Connected Successfully":
        print("Connected successfully, waiting for authentication...")

    # 处理认证结果
    elif data.get("resAc") == "auth":
        if data.get("code") == 1:
            print("Authentication successful")
            # 认证成功后订阅数据
            subscribe(ws)
        else:
            print("Authentication failed")
            ws.close()

    # 处理订阅结果
    elif data.get("resAc") == "subscribe":
        if data.get("code") == 1:
            print("Subscription successful")
        else:
            print("Subscription failed:", data.get("msg"))

    # 处理市场数据
    elif data.get("data"):
        # 打印实时行情数据
        market_data = data["data"]
        data_type = market_data.get("type")
        symbol = market_data.get("s")
        print(f"{data_type.upper()} data for {symbol}:", market_data)

def on_error(ws, error):
    """处理错误"""
    print("Error:", error)

def on_close(ws, close_status_code, close_msg):
    """连接关闭回调"""
    print("Connection closed")

def on_open(ws):
    """连接建立后的回调"""
    print("WebSocket connection opened")

def subscribe(ws):
    """订阅行情数据"""
    subscribe_msg = {
        "ac": "subscribe",
        "params": "EURUSD$GB",
        "types": "tick,quote,depth"
    }
    ws.send(json.dumps(subscribe_msg))
    print("Subscribe message sent")

def send_ping(ws):
    """定期发送心跳包"""
    while True:
        time.sleep(30)  # 每30秒发送一次心跳
        ping_msg = {
            "ac": "ping",
            "params": str(int(time.time() * 1000))
        }
        ws.send(json.dumps(ping_msg))
        print("Ping sent")

if __name__ == "__main__":
    # 创建 WebSocket 连接，通过header传递token
    ws = websocket.WebSocketApp(
        WS_URL,
        header={"token": API_TOKEN},
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    # 在单独的线程中启动心跳机制
    ping_thread = threading.Thread(target=send_ping, args=(ws,))
    ping_thread.daemon = True
    ping_thread.start()

    # 启动 WebSocket 连接
    ws.run_forever()

```

## Java 示例

```java
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SimpleITickWebSocketClient implements WebSocket.Listener {

    private WebSocket webSocket;
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // WebSocket连接地址和token
    private static final String WS_URL = "wss://api.itick.org/forex";
    private static final String API_TOKEN = "your_token";

    public static void main(String[] args) {
        SimpleITickWebSocketClient client = new SimpleITickWebSocketClient();
        client.connect();

        // 保持程序运行
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void connect() {
        HttpClient client = HttpClient.newHttpClient();

        WebSocket.Builder builder = client.newWebSocketBuilder();
        builder.header("token", API_TOKEN);

        webSocket = builder.buildAsync(URI.create(WS_URL), this).join();
        System.out.println("Connected to WebSocket");

        // 启动心跳机制
        startHeartbeat();
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        System.out.println("WebSocket connection opened");
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        System.out.println("Received: " + data);

        // 解析消息
        String message = data.toString();
        if (message.contains("\"msg\":\"Connected Successfully\"")) {
            System.out.println("Connected successfully");
        } else if (message.contains("\"resAc\":\"auth\"") && message.contains("\"code\":1")) {
            System.out.println("Authenticated successfully");
            // 认证成功后订阅数据
            subscribe();
        } else if (message.contains("\"resAc\":\"subscribe\"") && message.contains("\"code\":1")) {
            System.out.println("Subscribed successfully");
        }

        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.err.println("WebSocket error: " + error.getMessage());
        error.printStackTrace();
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        System.out.println("WebSocket closed: " + reason);
        scheduler.shutdown();
        return CompletableFuture.completedFuture(null);
    }

    private void subscribe() {
        String subscribeMessage = "{\n" +
                "  \"ac\": \"subscribe\",\n" +
                "  \"params\": \"EURUSD$GB\",\n" +
                "  \"types\": \"tick,quote,depth\"\n" +
                "}";

        webSocket.sendText(subscribeMessage, true);
        System.out.println("Subscribe message sent");
    }

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            String pingMessage = "{\n" +
                    "  \"ac\": \"ping\",\n" +
                    "  \"params\": \"" + System.currentTimeMillis() + "\"\n" +
                    "}";
            webSocket.sendText(pingMessage, true);
            System.out.println("Ping sent");
        }, 30, 30, TimeUnit.SECONDS); // 每30秒发送一次心跳
    }
}
```

## go 示例

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
	"github.com/gorilla/websocket"
)

const (
	// WebSocket连接地址和token
	WS_URL     = "wss://api.itick.org/forex"
	API_TOKEN  = "your_token"
)

// 消息结构体定义
type Message struct {
	Code   int         `json:"code,omitempty"`
	Msg    string      `json:"msg,omitempty"`
	ResAc  string      `json:"resAc,omitempty"`
	Data   interface{} `json:"data,omitempty"`
	Ac     string      `json:"ac,omitempty"`
	Params string      `json:"params,omitempty"`
	Types  string      `json:"types,omitempty"`
}

func main() {
	// 创建WebSocket连接
	headers := make(map[string][]string)
	headers["token"] = []string{API_TOKEN}

	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(WS_URL, headers)
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer conn.Close()

	fmt.Println("Connected to WebSocket")

	// 启动心跳协程
	go sendHeartbeat(conn)

	// 处理接收消息
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			return
		}

		fmt.Printf("Received: %s\n", message)

		// 解析消息
		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Println("Unmarshal error:", err)
			continue
		}

		// 处理连接成功的消息
		if msg.Code == 1 && msg.Msg == "Connected Successfully" {
			fmt.Println("Connected successfully")
		} else if msg.ResAc == "auth" && msg.Code == 1 {
			// 认证成功后订阅数据
			fmt.Println("Authenticated successfully")
			subscribe(conn)
		} else if msg.ResAc == "subscribe" && msg.Code == 1 {
			fmt.Println("Subscribed successfully")
		}
	}
}

// 订阅数据
func subscribe(conn *websocket.Conn) {
	subscribeMsg := Message{
		Ac:     "subscribe",
		Params: "EURUSD$GB",
		Types:  "tick,quote,depth",
	}

	message, err := json.Marshal(subscribeMsg)
	if err != nil {
		log.Println("Marshal error:", err)
		return
	}

	err = conn.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println("Write error:", err)
		return
	}

	fmt.Println("Subscribe message sent")
}

// 发送心跳包
func sendHeartbeat(conn *websocket.Conn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		pingMsg := Message{
			Ac:     "ping",
			Params: fmt.Sprintf("%d", time.Now().UnixNano()/int64(time.Millisecond)),
		}

		message, err := json.Marshal(pingMsg)
		if err != nil {
			log.Println("Marshal error:", err)
			continue
		}

		err = conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			log.Println("Write error:", err)
			return
		}

		fmt.Println("Ping sent")
	}
}

```

## Node JS 示例

```js
const WebSocket = require("ws");

// WebSocket连接地址和token
const WS_URL = "wss://api.itick.org/forex";
const API_TOKEN = "your_token";

// 创建WebSocket连接
const ws = new WebSocket(WS_URL, {
  headers: {
    token: API_TOKEN,
  },
});

// 连接打开事件
ws.on("open", function open() {
  console.log("Connected to WebSocket");
});

// 接收消息事件
ws.on("message", function message(data) {
  console.log("Received:", data.toString());

  const response = JSON.parse(data.toString());

  // 处理连接成功的消息
  if (response.code === 1 && response.msg === "Connected Successfully") {
    console.log("Connected successfully");
  }
  // 处理认证结果
  else if (response.resAc === "auth") {
    if (response.code === 1) {
      console.log("Authenticated successfully");
      // 认证成功后订阅数据
      subscribe();
    } else {
      console.log("Authentication failed");
      ws.close();
    }
  }
  // 处理订阅结果
  else if (response.resAc === "subscribe") {
    if (response.code === 1) {
      console.log("Subscribed successfully");
    } else {
      console.log("Subscription failed:", response.msg);
    }
  }
  // 处理市场数据
  else if (response.data) {
    const marketData = response.data;
    const dataType = marketData.type || "unknown";
    const symbol = marketData.s || "unknown";
    console.log(`${dataType.toUpperCase()} data for ${symbol}:`, marketData);
  }
});

// 错误处理
ws.on("error", function error(error) {
  console.error("WebSocket error:", error);
});

// 连接关闭事件
ws.on("close", function close() {
  console.log("Connection closed");
});

// 订阅数据函数
function subscribe() {
  const subscribeMsg = {
    ac: "subscribe",
    params: "EURUSD$GB",
    types: "tick,quote,depth",
  };

  ws.send(JSON.stringify(subscribeMsg));
  console.log("Subscribe message sent");
}

// 发送心跳包
setInterval(() => {
  const pingMsg = {
    ac: "ping",
    params: Date.now().toString(),
  };

  ws.send(JSON.stringify(pingMsg));
  console.log("Ping sent");
}, 30000); // 每30秒发送一次心跳

console.log("Connecting to iTick WebSocket...");
```
