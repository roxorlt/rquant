<!-- source: https://docs.itick.org/zh-cn/getting-started.md -->
<!-- fetched: 2026-07-28 | locale: zh-cn | mode: md | slug: getting-started -->

---
title: 快速开始
description: 开始使用iTick API：新手指南提供了从创建账户、生成API Key到执行第一个请求的完整教程。包含Python/Java/Go等代码片段，立即体验
keywords: 快速入门, 轻松集成, 获取授权, API密钥, 代码示例, RESTful
---

# 快速开始

## 1、账号准备

打开官网 [iTick Website](https://itick.org/zh-cn){:target="\_blank"}，点击右上角的立即获取

![iTick 首页](https://docs.itick.org/images/getting-started/zh-cn/home.webp)

使用 Google账号或者Github账号登录

![iTick 登录](https://docs.itick.org/images/getting-started/zh-cn/login.webp)

## 2、APIKey获取

登陆之后，访问 [iTick 控制台](https://itick.org/zh-hk/dashboard){:target="\_blank"}，点击控制台进入控制台。查看APIKey。
APIKey为每个账号唯一作为访问服务的凭证，请妥善保管，谨防丢失或共享他人，造成不必要的损失。如有以上情形请及时联系工作人员。

![iTick 操作指引](https://docs.itick.org/images/getting-started/zh-cn/guidelines.webp)

![iTick 控制台](https://docs.itick.org/images/getting-started/zh-cn/dashboard.webp)

## 3、服务简介

目前iTick支持rest服务、webs socket订阅以及FIX协议，三种种方式对外服务；
rest api：股票（A股、美股、港股）、外汇、贵金属、加密货币、指数的最新报价、盘口以及各种周期的K线；
webSocket api：股票（A股、美股、港股）、外汇、贵金属、加密货币、指数的实时报价推送；
fix api：目前支只对机构用户开放；
你可以根据实际情况选择不同的产品服务。
