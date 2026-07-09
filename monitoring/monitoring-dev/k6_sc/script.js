import http from 'k6/http';
import { sleep, check } from 'k6';

// 压测配置
export const options = {
    vus: 100,
    duration: '60s',
};

export default function () {
    const url = "http://172.16.111.60:1108/api/v1/hello";
    const payload = JSON.stringify({
        name: "test",
        msg: "k6 post test"
    });
    const params = {
        headers: {
            "Content-Type": "application/json",
        },
        timeout: "2s"
    };

    const res = http.post(url, payload, params);

    check(res, {
        "status=200": (r) => r.status === 200,
        "body not empty": (r) => r.body.length > 0,
    });

    sleep(0.1);
}