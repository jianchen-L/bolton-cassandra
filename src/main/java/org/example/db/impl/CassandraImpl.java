package org.example.db.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
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
                .withClusteringColumn("lock_ts", DataTypes.TIMESTAMP)
                .withColumn("tid", DataTypes.BIGINT);
        CreateTable txnInfo = SchemaBuilder.createTable("ramp", "txn_info").ifNotExists()
                .withPartitionKey("tid", DataTypes.TEXT)
                .withClusteringColumn("info_ts", DataTypes.TIMESTAMP)
                .withColumn("write_set", DataTypes.setOf(DataTypes.TEXT));
        Insert txnLockWriteBuilder = QueryBuilder.insertInto("ramp", "txn_lock")
                .value("key", QueryBuilder.bindMarker())
                .value("lock_ts", QueryBuilder.bindMarker())
                .value("tid", QueryBuilder.bindMarker());
//        session.execute(ramp.build());
//        session.execute(txnLock.build());
//        session.execute(txnInfo.build());
//        txnLockWrite = session.prepare(txnLockWriteBuilder.build());
    }

    @Override
    public void txnLockMeta(String key, Instant timestamp, long tid) {
        session.execute(txnLockWrite.bind(key, timestamp, tid));
    }

    public ResultSet read(CqlInfo cqlInfo) {
        // TODO:关联读取结果的key和值
        Set<Term> keysCast = new HashSet<>();
        Map<String, Row> result = new HashMap<>();
        Select selectResult = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns());
        for (ColumnMetadata primaryColumn : cqlInfo.getPrimaryColumns()) {
            selectResult = selectResult.whereColumn(primaryColumn.getName()).isEqualTo(QueryBuilder.bindMarker());
        }
        PreparedStatement preparedSelectResult = session.prepare(selectResult.build());
        for (Map.Entry<String, Row> key : cqlInfo.getKeys().entrySet()) {
            keysCast.add(QueryBuilder.literal(key.getKey()));
            Row keyRow = key.getValue();
            BoundStatementBuilder boundStatementBuilder = preparedSelectResult.boundStatementBuilder();
            for (int i = 0; i < keyRow.size(); i++) {
                boundStatementBuilder.set(i, keyRow.get(i), keyRow.)
            }
            preparedSelectResult.boundStatementBuilder()
            result.put(key, session.execute(preparedSelectResult.bind(primaryValues)).one());
        }

        Select selectUpdatedByTxn = QueryBuilder.selectFrom("ramp", "txn_lock").all().whereColumn("key").in(keysCast);
        ResultSet updatedByTxn = session.execute(selectUpdatedByTxn.build());
        if (updatedByTxn.all().isEmpty()) {
            return session.execute(cqlInfo.getRaw());
        } else {
            while (!session.isClosed()) {
                Map<String, Instant> latest = new HashMap<>();
                for (Row updatedByTxnRow : updatedByTxn) {
                    Select selectTxnMetadata = QueryBuilder.selectFrom("ramp", "txn_info").all().whereColumn("tid").isEqualTo(QueryBuilder.literal(updatedByTxnRow.getLong("tid")));
                    Row txnMetadata = session.execute(selectTxnMetadata.build()).one();
                    Set<String> writeSet = txnMetadata.getSet("write_set", String.class);
                    Set<Term> writeSetCast = new HashSet<>();
                    for (String s : writeSet) {
                        writeSetCast.add(QueryBuilder.literal(s));
                    }
                    Select selectWriteSetLock = QueryBuilder.selectFrom("ramp", "txn_lock").column("key").whereColumn("key").in(writeSetCast);
                    ResultSet writeSetLock = session.execute(selectWriteSetLock.build());
                    for (Row writeSetLockRow : writeSetLock) {
                        String key = writeSetLockRow.getString("key");
                        Instant ts = txnMetadata.getInstant("info_ts");
                        if (latest.getOrDefault(key, Instant.MIN).isBefore(ts)) {
                            latest.put(key, ts);
                        }
                    }
                }
                Map<Term, Instant> needed = new HashMap<>();
                for (Row updatedByTxnRow : updatedByTxn) {
                    String key = updatedByTxnRow.getString("key");
                    Instant latestOfKey = latest.get(key);
                    if (latestOfKey.isAfter(updatedByTxnRow.getInstant("lock_ts"))) {
                        needed.put(QueryBuilder.literal(key), latestOfKey);
                    }
                }
                Set<Term> newer = new HashSet<>();
                while (!needed.isEmpty()) {
                    Select selectNeeded = QueryBuilder.selectFrom("ramp", "txn_lock").columns("key", "lock_ts").whereColumn("key").in(needed.keySet());
                    ResultSet neededResult = session.execute(selectNeeded.build());
                    for (Row neededRow : neededResult) {
                        Term key = QueryBuilder.literal(neededRow.getString("key"));
                        Instant ts = neededRow.getInstant("lock_ts");
                    }
                }
            }
        }
        return null;
    }
}
