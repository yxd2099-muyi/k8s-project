在这个文件夹下执行命令
```shell
docker compose run --rm k6 run --out experimental-prometheus-rw /scripts/script.js
```

如果是在gitbash 下执行
```shell
docker compose run --rm k6 run --out experimental-prometheus-rw //scripts/script.js
```

在浏览器中输入
http://localhost:9090

在查询框执行
k6_http_req_duration_seconds
会看到相关数据 说明正确
