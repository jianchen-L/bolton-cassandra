package org.example.db.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import org.example.common.CqlInfo;
import org.example.db.DBStrategy;

import java.time.Instant;
import java.util.*;

public class CassandraImpl implements DBStrategy {

    private final CqlSession session;
    private PreparedStatement txnLockWrite;

    public CassandraImpl() {
        session = CqlSession.builder().build();
        CreateKeyspace ramp = SchemaBuilder.createKeyspace("ramp").ifNotExists().withSimpleStrategy(3);
        CreateTable txnLock = SchemaBuilder.createTable("ramp", "txn_lock").ifNotExists()
                .withPartitionKey("key", DataTypes.TEXT)
                .withColumn("lock_ts", DataTypes.TIMESTAMP)
                .withColumn("tid", DataTypes.BIGINT);
        CreateTable txnInfo = SchemaBuilder.createTable("ramp", "txn_info").ifNotExists()
                .withPartitionKey("tid", DataTypes.BIGINT)
                .withColumn("info_ts", DataTypes.TIMESTAMP)
                .withColumn("write_set", DataTypes.setOf(DataTypes.TEXT));
        Insert txnLockWriteBuilder = QueryBuilder.insertInto("ramp", "txn_lock")
                .value("key", QueryBuilder.bindMarker())
                .value("lock_ts", QueryBuilder.bindMarker())
                .value("tid", QueryBuilder.bindMarker());
        session.execute(ramp.build());
        session.execute(txnLock.build());
        session.execute(txnInfo.build());
//        txnLockWrite = session.prepare(txnLockWriteBuilder.build());
    }

    @Override
    public void txnLockMeta(String key, Instant timestamp, long tid) {
        session.execute(txnLockWrite.bind(key, timestamp, tid));
    }

    public Collection<String> read(CqlInfo cqlInfo) {
        // 关联读取结果的key和值
        Set<Term> keysCast = new HashSet<>();
        Map<String, String> result = new HashMap<>();
        Select selectResult = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns());
        for (ColumnMetadata primaryColumn : cqlInfo.getPrimaryColumns()) {
            selectResult = selectResult.whereColumn(primaryColumn.getName()).isEqualTo(QueryBuilder.bindMarker());
        }
        PreparedStatement preparedSelectResult = session.prepare(selectResult.build());
        for (Map.Entry<String, Row> key : cqlInfo.getKeys().entrySet()) {
            keysCast.add(QueryBuilder.literal(key.getKey()));
            BoundStatementBuilder boundStatementBuilder = preparedSelectResult.boundStatementBuilder();
            Row keyRow = key.getValue();
            for (int i = 0; i < keyRow.size(); i++) {
                TypeCodec<Object> codec = boundStatementBuilder.codecRegistry().codecFor(keyRow.getType(i));
                boundStatementBuilder.set(i, keyRow.get(i, codec), codec);
            }
            result.put(key.getKey(), session.execute(boundStatementBuilder.build()).one().getFormattedContents());
        }
        // 原子可见性检查
        // 检查被事务更新过的数据项
        Select selectUpdatedByTxn = QueryBuilder.selectFrom("ramp", "txn_lock").all().whereColumn("key").in(keysCast);
        List<Row> updatedByTxnList = session.execute(selectUpdatedByTxn.build()).all();
        if (updatedByTxnList.isEmpty()) {
            return result.values();
        } else {
            Select selectTxnMetadata = QueryBuilder.selectFrom("ramp", "txn_info").all().whereColumn("tid").isEqualTo(QueryBuilder.bindMarker());
            PreparedStatement preparedSelectTxnMetadata = session.prepare(selectTxnMetadata.build());
            Select selectWriteSetLock = QueryBuilder.selectFrom("ramp", "txn_lock").column("key").whereColumn("key").in(QueryBuilder.bindMarker());
            PreparedStatement preparedSelectWriteSetLock = session.prepare(selectWriteSetLock.build());
            Select selectNeeded = QueryBuilder.selectFrom("ramp", "txn_lock").columns("key", "lock_ts").whereColumn("key").in(QueryBuilder.bindMarker());
            PreparedStatement preparedSelectNeeded = session.prepare(selectNeeded.build());
            while (!session.isClosed()) {
                // 从事务元数据获取数据项的最新时间戳
                Map<String, Instant> latest = new HashMap<>(updatedByTxnList.size());
                for (Row updatedByTxnRow : updatedByTxnList) {
                    Row txnMetadata = session.execute(preparedSelectTxnMetadata.bind(updatedByTxnRow.getLong("tid"))).one();
                    Set<String> writeSet = txnMetadata.getSet("write_set", String.class);
                    List<String> writeSetCast = new LinkedList<>(writeSet);
                    ResultSet writeSetLock = session.execute(preparedSelectWriteSetLock.bind(writeSetCast));
                    for (Row writeSetLockRow : writeSetLock) {
                        String key = writeSetLockRow.getString("key");
                        Instant ts = txnMetadata.getInstant("info_ts");
                        if (latest.getOrDefault(key, Instant.MIN).isBefore(ts)) {
                            latest.put(key, ts);
                        }
                    }
                }
                // 获取需要等待复制的数据项
                Map<String, Instant> needed = new HashMap<>(updatedByTxnList.size());
                for (Row updatedByTxnRow : updatedByTxnList) {
                    String key = updatedByTxnRow.getString("key");
                    Instant latestOfKey = latest.get(key);
                    if (latestOfKey.isAfter(updatedByTxnRow.getInstant("lock_ts"))) {
                        needed.put(key, latestOfKey);
                    }
                }
                // 读取等待复制数据项的最新数据，如果读取到更晚的时间戳则循环读取丢失的数据项
                Set<Term> newer = new HashSet<>();
                // 维护一个int值来动态设置等待时间
                int loopCount = 0;
                while (!needed.isEmpty()) {
                    if (loopCount++ > 0) {
                        try {
                            Thread.sleep(10L << loopCount);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    ResultSet neededResult = session.execute(preparedSelectNeeded.bind(new LinkedList<>(needed.keySet())));
                    Map<String, Row> keys = cqlInfo.getKeys();
                    for (Row neededRow : neededResult) {
                        String key = neededRow.getString("key");
                        Instant ts = neededRow.getInstant("lock_ts");
                        int compare = latest.get(key).compareTo(ts);
                        if (compare == 0) {
                            needed.remove(key);
                            BoundStatementBuilder boundStatementBuilder = preparedSelectResult.boundStatementBuilder();
                            Row keyRow = keys.get(key);
                            for (int i = 0; i < keyRow.size(); i++) {
                                TypeCodec<Object> codec = boundStatementBuilder.codecRegistry().codecFor(keyRow.getType(i));
                                boundStatementBuilder.set(i, keyRow.get(i, codec), codec);
                            }
                            result.put(key, session.execute(boundStatementBuilder.build()).one().getFormattedContents());
                        } else if (compare < 0) {
                            needed.remove(key);
                            newer.add(QueryBuilder.literal(key));
                        }
                    }
                }
                if (newer.size() == 0) {
                    break;
                } else {
                    selectUpdatedByTxn = QueryBuilder.selectFrom("ramp", "txn_lock").all().whereColumn("key").in(newer);
                    updatedByTxnList = session.execute(selectUpdatedByTxn.build()).all();
                }
            }
        }
        return result.values();
    }
}
