const assert = require('assert');
const blue_redis = require('./index');

const redisClient = blue_redis.redisClient();
const redisAddr = redisClient.address;
console.log(redisAddr);


const testExist = async (key) => {
    let is_existed = await redisClient.existsAsync(key);
    if (is_existed) {
        let r = await redisClient.delAsync(key);
        is_existed = await redisClient.existsAsync(key);
        assert.ok(!is_existed);
        console.log('[redis] del', r);
    }
};

const testSet = async function (key) {
    let v = 'xxx';
    let r = await redisClient.setAsync(key, v);
    let v1 = await redisClient.getAsync(key);
    let is_existed = await redisClient.existsAsync(key);
    assert.ok(is_existed);
    assert.equal(v, v1);
    console.log('[redis] Set', r);
};

const testExpire = async function (key) {
    // 先不要考虑 自动 expire
    let secs = 3;
    redisClient.expireAsync(key, secs);
    console.log('expire at', new Date(), secs);
    setTimeout(async () => {
        let is_existed = await redisClient.existsAsync(key);
        assert.ok(!is_existed);
        console.log('expire success', new Date())
    }, (secs + 2) * 1000);
};

const testSadd = async (key) => {
    let d1 = await redisClient.saddAsync(key, 111);
    let d2 = await redisClient.saddAsync(key, 222, 3333, 4444);
    let ds = await redisClient.smembersAsync(key);

    console.log('redis sets', ds, d1, d2);
    let d3 = await redisClient.saddAsync(key, [3333, 31434, 3132]);
    let ds2 = await redisClient.smembersAsync(key);

    let d4 = await redisClient.sremAsync(key, 3333);
    let s4 = await redisClient.smembersAsync(key);

    console.log('redis sets', ds2, d3);
    let d5 = await redisClient.delAsync(key);
    let ds3 = await redisClient.smembersAsync(key);
    console.log('redis sets del', key, d4, ds3);
};

const testDB = async () => {
    const db_key = 'test_key';

    await testExist(db_key);
    await testSet(db_key);
    await testExpire(db_key);

    const rdb = new blue_redis.redisDB(db_key, redisClient);
    let r1 = rdb.freeAsync();
    console.log('free', r1, db_key);
    await testSadd(db_key);
    await testExist(db_key);
};

const testList = async () => {
    const db_key = 't_list';
    const rdb = new blue_redis.redisList(db_key, redisClient);
    const id = rdb.lpushAsync(111);
    rdb.lpushAsync(1, 2, 3, 4).then(res => {
        console.log('lpush', res);
        rdb.rpopAsync().then(res => {
            console.log('rpop', res)
        })
    });

    const ds = await rdb.rangeAsync();
    console.log(ds);
    rdb.rangeAsync().then(res => {
        console.log('all', res);
        rdb.blpopAsync().then(res => {
            console.log('blpop', res);
            rdb.blpopAsync().then(res => {
                console.log('blpop', res);
                rdb.brpopAsync().then(res => {
                    console.log('brpop', res);
                    rdb.brpopAsync().then(res => {
                        console.log('brpop', res);
                        rdb.rpopAsync().then(res => {
                            console.log('brpop', res);
                            rdb.freeAsync().then(res => {
                                console.log('free', res)
                            })
                        });
                    });
                });
            })
        })
    });
};

const testHash = async () => {
    const db_key = 'test_hash';
    const value = 'minieyeTest';
    const rdb = new blue_redis.redisHash(db_key, redisClient);
    const d0 = await rdb.hgetAsync('name');
    console.log(db_key, d0);
    if (d0 === null) {
        const d1 = await rdb.hsetAsync('name', value);
        const d2 = await rdb.hgetAsync('name');
        console.log('hset', d1);
        assert.equal(d2, value);
    } else {
        const d1 = await rdb.hdelAsync('name');
        const d2 = await rdb.hgetAsync('name');
        console.log('hdel', d1);
        assert.equal(d2, null);
    }

    const d3 = await rdb.freeAsync();    // 0 or 1
    const d4 = await rdb.existsAsync();  // 0 or 1
    console.log('free', d3, d4);
    assert.equal(d4, 0);
};

const testSets = async () => {
    const db_key = 'test_sets';
    const rdb = new blue_redis.redisSets(db_key, redisClient);
    const ds = await rdb.allAsync();
    console.log(db_key, ds);
    assert.ok(ds instanceof Array);
    // assert.ok(ds.length === 0);
    const d1 = await rdb.addAsync(111);
    const d2 = await rdb.addAsync(222);
    const ds1 = await rdb.allAsync();
    assert.equal(d1, 1);
    assert.equal(d2, 1);

    const d3 = await rdb.addAsync(111);
    const ds2 = await rdb.allAsync();
    assert.equal(d3, 0);
    assert.equal(ds1.length, ds2.length);
    console.log(db_key, 'all', ds2);

    const d4 = await rdb.hasAsync(111);
    assert.equal(d4, 1);
    const d5 = await rdb.delAsync(111);
    assert.equal(d5, 1);
    const d6 = await rdb.hasAsync(111);
    assert.equal(d6, 0);

    const d7 = await rdb.freeAsync();    // 0 or 1
    const d8 = await rdb.existsAsync();  // 0 or 1
    console.log('free', d7, d8);
    assert.equal(d8, 0);

};

const testZset = async () => {
    const db_key = 'test_zset';
    const rdb = await new blue_redis.redisZset(db_key, redisClient);
    const size = await rdb.sizeAsync();
    console.log(db_key, size);
    assert.equal(size, 0);

    const ms = [100, 'Hujia', 80, 'sth', 70, 'mfg', 80, 'dev'];
    const d1 = await rdb.zaddAsync(...ms);
    assert.equal(d1, 4);
    const d2 = await rdb.zaddAsync(...ms);
    assert.equal(d2, 0);

    const d3 = await rdb.zaddAsync(85, 'test');
    assert.equal(d3, 1);
    const d4 = await rdb.sizeAsync();
    assert.equal(d4, 5);

    const d5 = await rdb.countAsync(80, 80);
    assert.equal(d5, 2);

    const sc = await rdb.zscoreAsync('dev');
    assert.equal(sc, 80);

    // default  递增
    const ds = await rdb.zrangeAsync();
    const ds1 = await rdb.rangeAsync();
    console.log('zrange', ds);
    console.log('range', ds1);

    const rnk = await rdb.zrankAsync('Hujia');
    const rank = await rdb.rankAsync('Hujia');
    console.log(rnk, rank);

    const rnk1 = await rdb.zrankAsync('dev');
    const rank1 = await rdb.rankAsync('dev');
    console.log(rnk1, rank1);

    const rnk2 = await rdb.zrankAsync('mfg');
    const rank2 = await rdb.rankAsync('mfg');
    console.log(rnk2, rank2);


    // reverse 递减
    const vds = await rdb.zrevrangeAsync();
    const vds1 = await rdb.revrangeAsync();
    console.log('zrevrange', vds);
    console.log('revrange', vds1);

    const vrnk = await rdb.zrevrankAsync('Hujia');
    const vrank = await rdb.revrankAsync('Hujia');
    console.log(vrnk, vrank);

    const vrnk1 = await rdb.zrevrankAsync('dev');
    const vrank1 = await rdb.revrankAsync('dev');
    console.log(vrnk1, vrank1);

    const vrnk2 = await rdb.zrevrankAsync('mfg');
    const vrank2 = await rdb.revrankAsync('mfg');
    console.log(vrnk2, vrank2);


    console.log(db_key, d5);
    const d7 = await rdb.freeAsync();    // 0 or 1
    const d8 = await rdb.existsAsync();  // 0 or 1
    console.log('free', d7, d8);
    assert.equal(d8, 0);
};

const testMain = async () => {
    await testDB();
    await testList();
    await testHash();
    await testSets();
    await testZset()
};

testMain().then(ret => {
    console.log(ret);
    console.log('finished!!!')
}).catch(err => {
    console.log('error!!!', err);
    throw err;
});
