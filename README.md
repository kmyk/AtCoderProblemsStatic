# AtCoderProblemsStatic

[![Travis](https://img.shields.io/travis/kmyk/online-judge-tools/master.svg)](https://travis-ci.org/kmyk/online-judge-tools)

<https://kimiyuki.net/AtCoderProblemsStatic/index.html#/table/kimiyuki/kenkoooo>

## これなに

AtCoderProblemsStatic は [kenkoooo/AtCoderProblems](https://github.com/kenkoooo/AtCoderProblems) と互換性のある API server です。
本家が高負荷のため維持費がつらそうなので、諸々を GitHub Pages 上に静的に配置することで解決をしました。

## 動かし方

``` console
# start the Postgres server
$ docker-compose up --no-start
$ docker-compose start database

# scrape the submissions from AtCoder
$ docker-compose up scraper

# export the submissions to export/dist/
$ docker-compose up database
```
