<img src="https://gw.alipayobjects.com/zos/antfincdn/R8sN%24GNdh6/language.svg" width="18"> [English](./README.md) | 简体中文

<h1 align="center">
<b>tdd-spider</b>
</h1>

<div align="center">
天钿Daily（<a href="https://tdd.bunnyxt.com">https://tdd.bunnyxt.com</a>）的数据获取程序，基于Python，融合<a href="https://github.com/Python3WebSpider/ProxyPool">ProxyPool</a>。QQ群：<a href="https://jq.qq.com/?_wv=1027&k=588s7nw">537793686</a>，欢迎加入！
</div>

## 简介

## 安装

1. 下载代码，`git clone https://github.com/bunnyxt/tdd-spider.git && cd tdd-spider`。
2. 配置`Python 3.5+`环境（建议使用`virtualenv`或`conda`新建虚拟环境以避免依赖冲突），运行`pip install -r requirements.txt`安装依赖。
3. 安装[ProxyPool](https://github.com/Python3WebSpider/ProxyPool) ，配置以下参数
    ```yaml
    CYCLE_TESTER: 10
    CYCLE_GETTER: 60
    TEST_URL: http://api.bilibili.com/x/web-interface/view?aid=456930
    TEST_TIMEOUT: 3
    TEST_BATCH: 100
    ```
    ，推荐使用`docker`安装，并在`docker-compose.yml`文件里面指定配置，配置完成后`nohup docker-compose up &`在后台启动ProxyPool服务。
4. // TODO
