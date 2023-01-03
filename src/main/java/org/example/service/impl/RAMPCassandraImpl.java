package org.example.service.impl;

import org.example.db.DBStrategy;
import org.example.db.impl.CassandraImpl;
import org.example.service.RAMPService;
import org.example.utils.JedisPoolUtil;
import redis.clients.jedis.Jedis;

public class RAMPCassandraImpl implements RAMPService {

    private Jedis jedis;
    private DBStrategy dbStrategy;

    public RAMPCassandraImpl() {
        jedis = JedisPoolUtil.getJedis();
        dbStrategy = new CassandraImpl();
    }

    @Override
    public void nonTxn(String nonTxnCqls) {
        String[] cqls = nonTxnCqls.split(";");
        for (String cql : cqls) {

        }
    }

    @Override
    public void txn(String s) {

    }

    private void select() {

    }

    private void upsert() {

    }

    private void delete() {
    }

    private long getDistributedId() {
        return jedis.incr("tid");
    }

    private enum CRUD {
        SELECT, UPSERT, DELETE;
    }
}
