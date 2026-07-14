# 第三方代码声明

## Tencent WeKnora DocReader

`app/docreader` 中的解析内核派生自 Tencent WeKnora：

- 上游项目：Tencent WeKnora
- 上游目录：`docreader`
- 来源提交：`ecaedf11966b1140073071677f260ef8c55e8d3d`
- 原始版权：Copyright (C) 2025 Tencent. All rights reserved.
- 许可证：MIT License

本项目移除了 gRPC、Proto、Go 客户端、网页解析、OpenDataLoader、旧 DOC 和其他当前未使用的集成，并调整了 Python 包导入路径和注册表。

MIT License：

> Permission is hereby granted, free of charge, to any person obtaining a copy
> of this software and associated documentation files (the "Software"), to deal
> in the Software without restriction, including without limitation the rights
> to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
> copies of the Software, and to permit persons to whom the Software is
> furnished to do so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
> OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
> SOFTWARE.

Python 第三方依赖仍分别适用其自身许可证。
