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

- 前端：数据展示与交互（[tdd-frontend](https://github.com/bunnyxt/tdd-frontend) ）
- 后端：数据获取接口（[tdd-backend](https://github.com/bunnyxt/tdd-backend) ）
- 爬虫：原始数据采集（tdd-spider ）

整体结构如图所示

![天钿Daily整体结构](./tdd-structure.png '天钿Daily整体结构')

## 安装

1. 下载代码，`git clone https://github.com/bunnyxt/tdd-spider.git && cd tdd-spider`。
2. 配置`Python 3.5+`环境（强烈推荐使用`virtualenv`或`conda`新建虚拟环境以避免依赖冲突），运行`pip install -r requirements.txt`安装依赖。
3. 安装[ProxyPool](https://github.com/Python3WebSpider/ProxyPool) ，为了尽可能提高IP的可用性，配置以下环境变量
    ```yaml
    CYCLE_TESTER: 10
    CYCLE_GETTER: 60
    TEST_URL: http://api.bilibili.com/x/web-interface/view?aid=456930
    TEST_TIMEOUT: 3
    TEST_BATCH: 100
    ```
    PS：推荐使用`docker`方式使用，并在`docker-compose.yml`文件底部`environment`之后粘贴以上环境变量配置，配置完成后使用`nohup docker-compose up &`在后台启动ProxyPool服务。
4. 打开`conf/conf.ini`文件，填写配置，包括数据库连接（`MySQL 5.7.30`）、Server酱SCKEY（获取方式见[Server酱首页](http://sc.ftqq.com/3.version) ）、ProxyPool地址（默认[http://localhost:5555/random](http://localhost:5555/random) ）等。

## 运行

// TODO

## 文档

// TODO

## 声明

本项目提供了一种全自动定时数据获取系统的构建思路与实现，供学习交流。由于本人能力有限，系统构建难免存在各种大小问题，请勿直接在生产环境使用，如有损失恕不负责。

**特别提醒**：请遵守各地法律法规，切勿使用本系统通过非法手段采集任何敏感信息，或进行任何非法活动，一切后果请自行承担。

如果你对我或者我的项目感兴趣，欢迎通过以下方式联系我：

- 新浪微博 [@牛奶源29](https://www.weibo.com/nny29)
- Twitter [@bunnyxt29](https://twitter.com/bunnyxt29)
- Email <a href="mailto:bunnyxt@outlook.com">bunnyxt@outlook.com</a>

by. bunnyxt 2021-01-12
