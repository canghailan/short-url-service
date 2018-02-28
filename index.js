const crypto = require('crypto');
const http = require('http');
const url = require('url');
const bluebird = require('bluebird');
const LRU = require("lru-cache");
const redis = require('redis');
const mysql = require('mysql');
const props = require('./props');

const port = props.port;
const minShortIdLength = props.minShortIdLength;
const cache = LRU(props.cache);
const redisClient = bluebird.promisifyAll(redis.createClient(props.redis));
const mysqlPool = bluebird.promisifyAll(mysql.createPool(props.mysql));

function urlSafe(base64) {
    return base64.replace(/[+/=]/g, $0 => {
        return {
            '+': '-',
            '/': '_',
            '=': ''
        }[$0];
    });
}

function sha256(text) {
    return crypto.createHash('sha256').update(text, 'utf8').digest();
}

function hashCode(sha256) {
    return parseInt(sha256.toString('hex').substring(0, 8), 16);
}

function readRequestBody(request) {
    return new Promise((resolve, reject) => {
        let chunks = [];
        request.on('error', reject);
        request.on('data', chunk => chunks.push(chunk));
        request.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

async function queryOneAsync(sql, params) {
    let r = await mysqlPool.queryAsync(sql, params);
    return r.length > 0 ? r[0] : null;
}

async function getURL(path) {
    let location = cache.get(path);
    if (location) {
        return location;
    }
    location = await redisClient.getAsync(path);
    if (location) {
        cache.set(path, location);
        return location;
    }
    let r = await queryOneAsync(`select * from url_mapping where path = ?`, [path]);
    if (r) {
        location = r.url;
    }
    if (location) {
        redisClient.setAsync(path, location).then(() => cache.set(path, location));
        return location;
    }
    return location;
}

async function putURL(mapping) {
    if (mapping.path) {
        mapping.path = mapping.path.replace(/^\//, '').trim();
    }
    if (mapping.url) {
        mapping.url = mapping.url.trim();
    }
    if (mapping.path) {
        let m = await queryOneAsync(`select * from url_mapping where path = ?`, [mapping.path]);
        if (m) {
            await mysqlPool.queryAsync(`update url_mapping set url = ?, last_update_time = now() where id = ?`, [mapping.url, m.id]);
        } else {
            if (/\//.test(mapping.path)) {
                await mysqlPool.queryAsync(
                    `insert into url_mapping (path, url, origin_url, create_time, last_update_time) values (?, ?, ?, now(), now())`,
                    [mapping.path, mapping.url, mapping.url]);
            } else {
                mapping.error = '指定Path必须包含/';
            }
        }
    } else if (mapping.url) {
        let hash = sha256(mapping.url);
        mapping.long_id = urlSafe(hash.toString('base64'));
        mapping.path = mapping.short_id = mapping.long_id.substring(0, minShortIdLength);
        let r = await mysqlPool.queryAsync(`select * from url_mapping where short_id like concat(?, '%') order by short_id`, [mapping.short_id]);
        if (r.length > 0) {
            let length = r[r.length - 1]['short_id'].length;
            mapping.path = mapping.short_id = mapping.long_id.substring(0, length);
            for (let i = 0; i < r.length; i++) {
                let m = r[i];
                if (m.url === mapping.url) {
                    mapping = m;
                }
            }
        }
        if (mapping.id == null) {
            await mysqlPool.queryAsync(
                `insert into url_mapping (short_id, path, url, origin_url, create_time, last_update_time) values (?, ?, ?, ?, now(), now())`,
                [mapping.short_id, mapping.path, mapping.url, mapping.url]);
        }
    } else {
        mapping.error = 'Path和URL不能同时为空';
    }
    if (mapping.path) {
        redisClient.delAsync(mapping.path).then(() => cache.del(mapping.path));
    }
    return mapping;
}

async function get(request, response) {
    let parsed = url.parse(request.url);
    let path = parsed.pathname.substring(1);
    let location = await getURL(path);

    if (location) {
        response.writeHead(302, {
            'Location': location
        });
        response.end();
    } else {
        response.statusCode = 404;
        response.end(`${request.method} ${request.url}`);
    }

    return arguments;
}

async function put(request, response) {
    let body = await readRequestBody(request);
    let list = JSON.parse(body.toString('utf-8'));
    let result = await Promise.all(list.map(putURL));

    response.writeHead(200, { 'Content-Type': 'application/json' });
    response.end(JSON.stringify(result, ['path', 'url', 'error'], 2));

    return arguments;
}

const router = {
    'GET': get,
    'POST': put
};

http.createServer(function (request, response) {
    console.log(`${request.method} ${request.url}`);
    let handler = router[request.method];
    if (handler) {
        handler(request, response).catch(function (e) {
            console.error(e);
            response.statusCode = 500;
            response.end();
        });
    } else {
        response.statusCode = 404;
        response.end();
    }
}).listen(port);