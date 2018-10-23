const redis = require('redis');
const bluebird = require('bluebird');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);


const redisClient = function (config = {}) {
    const cfg = {
        host: config.host || '127.0.0.1',
        port: config.port || 6379,
        db: parseInt(config.db) || 0,
    };
    const url = `redis://${cfg.host}:${cfg.port}/${cfg.db}`;
    console.log(`[BlueRedis] connecting to ${url} ...`);
    return redis.createClient(cfg);
};


class redisDB {
    constructor(key, client) {
        const name = this.constructor.name;
        if (!key) {
            throw new Error(`[${name}] init failed, invalid key <str:index>`)
        }
        if (!client) {
            throw new Error(`[${name}] init failed, invalid client <object:result of redisClient()>`)
        }
        this.db_key = key;
        this.client = client;
    }

    async freeAsync() {
        return await this.client.delAsync(this.db_key);
    }

    async expireAsync(seconds) {
        return await this.client.expireAsync(this.db_key, seconds);
    }

    async expireAtAsync(ts) {
        ts = parseInt(ts);
        if (String(ts).length === 13) {
            ts = parseInt(ts / 1000);
        }
        return await this.client.expireatAsync(this.db_key, ts);
    }

    async existsAsync() {
        return await this.client.existsAsync(this.db_key);
    }
}


class redisSets extends redisDB {
    constructor(key, client) {
        super(key, client);
    }

    async allAsync() {
        return await this.client.smembersAsync(this.db_key);
    }

    async hasAsync(name) {
        return await this.client.sismemberAsync(this.db_key, name)
    }

    async addAsync(name) {
        return await this.client.saddAsync(this.db_key, name);
    }

    async delAsync(name) {
        return await this.client.sremAsync(this.db_key, name);
    }
}


// storage of list encoded by JSON
class redisList extends redisDB {
    constructor(key, client) {
        super(key, client);
    }

    async lpushAsync(...datas) {
        const items = datas.map(m => JSON.stringify(m));
        return await this.client.lpushAsync(this.db_key, items);
    }

    async rpopAsync() {
        const value = await this.client.rpopAsync(this.db_key);
        return JSON.parse(value);
    }

    async blpopAsync(timeout = 30) {
        // null or ["${db_key}", "${value}"]
        const item = await this.client.blpopAsync(this.db_key, timeout);
        if (item !== null) {
            return JSON.parse(item[1])
        }
        return item;
    }

    async brpopAsync(timeout = 30) {
        // null or ["${db_key}", "${value}"]
        const item = await this.client.brpopAsync(this.db_key, timeout);
        if (item !== null) {
            return JSON.parse(item[1])
        }
        return item;
    }

    async rangeAsync(start = 0, end = -1) {
        const items = await this.client.lrangeAsync(this.db_key, start, end);
        return items.map(m => JSON.parse(m));
    }
}


// storage of hash table, with items encoded by JSON
class redisHash extends redisDB {
    constructor(key, client) {
        super(key, client);
    }

    async hallAsync() {
        const ds = await this.client.hgetallAsync(this.db_key);
        const result = {};
        for (let index in ds) {
            const text = ds[index];
            result[index] = JSON.parse(text);
        }
        return result;
    }

    async hgetAsync(index) {
        let data = await this.client.hgetAsync(this.db_key, index);
        if (data !== null) {
            data = JSON.parse(data);
        }
        return data;
    }

    async hsetAsync(index, data) {
        if (data) {
            return await this.client.hsetAsync(this.db_key, index, JSON.stringify(data))
        }
    }

    async hdelAsync(index) {
        return await this.client.hdelAsync(this.db_key, index);
    }
}


class redisZset extends redisDB {
    constructor(key, client) {
        super(key, client);
    }

    async sizeAsync() {
        return await this.client.zcardAsync(this.db_key);
    }

    async countAsync(min_score, max_score) {
        return await this.client.zcountAsync(this.db_key, min_score, max_score);
    }

    async zaddAsync(...score_and_indexes) {
        return await this.client.zaddAsync(this.db_key, ...score_and_indexes);
    }

    async zscoreAsync(index) {
        return await this.client.zscoreAsync(this.db_key, index);
    }

    async zrankAsync(index) {
        return await this.client.zrankAsync(this.db_key, index);
    }

    async zrangeAsync(from_rnk = 0, end_rnk = -1, with_scores = true) {
        let ds;
        if (with_scores) {
            ds = await this.client.zrangeAsync(this.db_key, from_rnk, end_rnk, 'withscores');
        } else {
            ds = await this.client.zrangeAsync(this.db_key, from_rnk, end_rnk)
        }
        return ds;
    }

    async rangeAsync(from_rnk = 0, end_rnk = -1) {
        const ds = await this.zrangeAsync.bind(this)(from_rnk, end_rnk, true);
        const data = [];
        let c_rank = 0;
        let c_score = 0;
        for (let i = 0; i <= ds.length / 2; i++) {
            let name = ds[i * 2];
            let score = ds[i * 2 + 1];
            if (score > c_score) {
                c_rank += 1;
                c_score = score;
            }
            let item = {name, score, rnk: from_rnk + i, rank: c_rank};
            data.push(item)
        }
        return data;
    }

    async rankAsync(index) {
        const rnk = await this.zrankAsync.bind(this)(index);
        if (rnk !== null) {
            const ds = await this.rangeAsync.bind(this)(0, rnk);
            const item = ds[rnk];
            return item.rank
        }
        return null;
    }

    /*
    *  默认序集成员按 score 值递增(从小到大)顺序排列
    *  rev 递减排列
    */

    async zrevrankAsync(index) {
        return await this.client.zrevrankAsync(this.db_key, index);
    }

    async zrevrangeAsync(from_rnk = 0, end_rnk = -1, with_scores = true) {
        let ds;
        if (with_scores) {
            ds = await this.client.zrevrangeAsync(this.db_key, from_rnk, end_rnk, 'withscores');
        } else {
            ds = await this.client.zrevrangeAsync(this.db_key, from_rnk, end_rnk)
        }
        return ds;
    }

    async revrangeAsync(from_rnk = 0, end_rnk = -1) {
        const ds = await this.zrevrangeAsync.bind(this)(from_rnk, end_rnk, true);
        const data = [];
        let c_rank = 0;
        let c_score = 0;
        for (let i = 0; i <= ds.length / 2; i++) {
            let name = ds[i * 2];
            let score = ds[i * 2 + 1];
            if (score < c_score) {
                c_rank += 1;
                c_score = score;
            }
            let item = {name, score, rnk: from_rnk + i, rank: c_rank};
            data.push(item)
        }
        return data;
    }

    async revrankAsync(index) {
        const rnk = await this.zrevrankAsync.bind(this)(index);
        if (rnk !== null) {
            const ds = await this.revrangeAsync.bind(this)(0, rnk);
            const item = ds[rnk];
            return item.rank
        }
        return null;
    }
}

module.exports = {
    _depends: {
        redis,
        bluebird,
    },
    redisClient: redisClient,
    redisDB: redisDB,
    redisSets: redisSets,
    redisHash: redisHash,
    redisZset: redisZset,
    redisList: redisList,
};
