<img src="https://gw.alipayobjects.com/zos/antfincdn/R8sN%24GNdh6/language.svg" width="18"> [English](./README.md) | 简体中文

<h1 align="center">
<b>tdd-spider</b>
</h1>

<div align="center">
天钿Daily（<a href="https://tdd.bunnyxt.com">https://tdd.bunnyxt.com</a>）的数据获取程序，基于Python，整合<a href="https://github.com/Python3WebSpider/ProxyPool">ProxyPool</a>代理池与<a href="http://sc.ftqq.com/3.version">Server酱</a>消息推送。QQ群：<a href="https://jq.qq.com/?_wv=1027&k=588s7nw">537793686</a>，欢迎加入！
</div>

## 简介

[天钿Daily](https://tdd.bunnyxt.com)为bunnyxt的个人项目，意在推进VC相关数据交流，为任何对VC数据感兴趣的用户提供尽可能完备且易得的数据及其可视化展示。

整个项目天然解耦为三个部分，通过中心数据库相连，这三个部分分别是：

- 前端：数据展示与交互（[tdd-frontend](https://github.com/bunnyxt/tdd-frontend)）
- 后端：数据获取接口（[tdd-backend](https://github.com/bunnyxt/tdd-backend)）
- 爬虫：原始数据采集（tdd-spider）

整体结构如图所示

![天钿Daily整体结构](./tdd-structure.png '天钿Daily整体结构')

## 安装

1. 下载代码，`git clone https://github.com/bunnyxt/tdd-spider.git && cd tdd-spider`。
2. 配置`Python 3.5+`环境（强烈推荐使用`virtualenv`或`conda`新建虚拟环境以避免依赖冲突），运行`pip install -r requirements.txt`安装依赖。
3. 安装[ProxyPool](https://github.com/Python3WebSpider/ProxyPool)，为了尽可能提高IP的可用性，配置以下环境变量
    ```yaml
    CYCLE_TESTER: 10
    CYCLE_GETTER: 60
    TEST_URL: http://api.bilibili.com/x/web-interface/view?aid=456930
    TEST_TIMEOUT: 3
    TEST_BATCH: 100
    ```
    PS：推荐使用`docker`方式使用，并在`docker-compose.yml`文件底部`environment`之后粘贴以上环境变量配置，配置完成后使用`nohup docker-compose up &`在后台启动ProxyPool服务。
4. 打开`conf/conf.ini`文件，填写配置，包括数据库连接（`MySQL 5.7.30`）、Server酱SCKEY（获取方式见[Server酱首页](http://sc.ftqq.com/3.version)）、ProxyPool地址（默认[http://localhost:5555/random](http://localhost:5555/random)）等。

## 运行

本全自动定时数据采集系统由一系列脚本组成，基本运行方法为：

```shell
python <script-name.py>
```

由于绝大多数脚本为定时脚本，即会每隔一段时间（或者在某个预设的时间点）运行，因此脚本需要常驻后台一直运行。`Linux`下建议使用`nohup + &`实现后台运行，即：

```shell
nohup python -u <script-name.py> >/dev/null 2>&1 &
```

说明：

- `python -u`表示强制脚本的标准输出也同标准错误一样不通过缓存直接打印到屏幕，此处建议设置`-u`以防止有时候出现日志没有及时输出的情况。
- 本系统绝大多数脚本使用配置过的`logging`输出日志，默认会将日志保存到`log`文件夹下，因此后台运行时，输出到控制台的日志完全可以直接丢弃，即`>/dev/null 2>&1`。

使用`nohup + &`将脚本后台运行后，需要使用`ps -aux | grep 'python -u <script-name.py>'`来查看运行状况，并通过`kill`结束进程的方式结束执行。

由于启动后台运行、查看运行情况、结束后台脚本运行等操作的执行频率很高，但指令很长容易打错，因此可以使用以下三个脚本简化操作：

启动后台运行

```shell script
./run_start.sh <script-name.py>
```

查看后台运行情况

```shell script
./run_ps.sh
```

结束后台脚本运行

```shell script
./run_kill.sh <pid>
```

## 脚本列表

本系统内置了一些定时数据获取或处理脚本，位于根目录下，文件名满足`数字+下划线+由短横线连接的一组英文单词+.py`格式，例如`16_daily-update-member-info.py`。这里对这些内置的脚本做一个简单的介绍。

首先解释一下文件名的含义：

- 下划线前的数字为`脚本编号`，通常功能相似的脚本由相同的编号前缀（如`0`开头的表示初始化脚本，`1`开头表示涉及到BiliBili API访问的定时脚本等等）。
- 下划线后的由`-`连接的一组英文单词为该脚本的功能介绍，供使用者快速了解脚本含义，节约文档查询时间。

// TODO

## 自定义脚本

内置脚本仅满足当前系统运行需要。当然，用户也可以自定义脚本，以满足未来的或者临时的需求。

注意：为了方便使用`run_xxx.sh`系列工具管理，建议仿照上文提到的内置脚本的命名规范，给自定义脚本命名。

// TODO 实质就是调用模块，给一个直接调用的例子，一个定时任务的例子，一个jupyter notebook的例子

## 模块文档

### common

### conf

### db

### pybiliapi

### serverchan

### spider

### util

// TODO

## 声明

本项目提供了一种全自动定时数据获取系统的构建思路与实现，供学习交流。由于本人能力有限，系统构建难免存在各种大小问题，请勿直接在生产环境使用，如有损失恕不负责。

**特别提醒**：请遵守各地法律法规，切勿使用本系统通过非法手段采集任何敏感信息，或进行任何非法活动，一切后果请自行承担。

如果你对我或者我的项目感兴趣，欢迎通过以下方式联系我：

- 新浪微博 [@牛奶源29](https://www.weibo.com/nny29)
- Twitter [@bunnyxt29](https://twitter.com/bunnyxt29)
- Email <a href="mailto:bunnyxt@outlook.com">bunnyxt@outlook.com</a>

by. bunnyxt 2021-01-14
