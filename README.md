<img src="https://gw.alipayobjects.com/zos/antfincdn/R8sN%24GNdh6/language.svg" width="18"> English | [简体中文](./README_zh-CN.md)

```
CAUTION: This program is related to VOCALOID CHINA, so the most of the documentations are written in Chinese. Sorry for inconvenience.
```

<h1 align="center">
<b>tdd-spider</b>
</h1>

<div align="center">
Data acquisition program of TianDian Daily (<a href="https://tdd.bunnyxt.com">https://tdd.bunnyxt.com</a>) based on Python, integrated with <a href="https://github.com/Python3WebSpider/ProxyPool">ProxyPool</a> proxy pool and <a href="http://sc.ftqq.com/3.version"> ServerChan </a> message pushing. Feel free to contact us via QQ group: <a href="https://jq.qq.com/?_wv=1027&k=588s7nw">537793686</a>！
</div>

## Introduction

[TianDian Daily](https://tdd.bunnyxt.com) is a personal project of bunnyxt, which hopes to enhance Vocaloid China related data exchange, providing complete and easily accessible data and data visualization for everyone who interests in VC data.

The project is made up with three different part, connect with center database, which are: 

- frontend: data presentation and interaction ([tdd-frontend](https://github.com/bunnyxt/tdd-frontend))
- backend: data access api ([tdd-backend](https://github.com/bunnyxt/tdd-backend))
- spider: original data collection（tdd-spider）

See the structure below

![TianDian Daily structure](./tdd-structure.png 'TianDian Daily structure')
