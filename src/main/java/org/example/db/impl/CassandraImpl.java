package org.example.db.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.example.common.CqlInfo;
import org.example.db.DBStrategy;

import java.time.Instant;

public class CassandraImpl implements DBStrategy {

    private final CqlSession session;
    private PreparedStatement txnLockWrite;

    public CassandraImpl() {
        session = CqlSession.builder().build();
        CreateKeyspace ramp = SchemaBuilder.createKeyspace("ramp").ifNotExists().withSimpleStrategy(3);
        CreateTable txnLock = SchemaBuilder.createTable("ramp", "txn_lock").ifNotExists()
                .withPartitionKey("key", DataTypes.TEXT)
                .withClusteringColumn("ts", DataTypes.TIMESTAMP)
                .withColumn("tid", DataTypes.BIGINT);
        CreateTable txnInfo = SchemaBuilder.createTable("ramp", "txn_info").ifNotExists()
                .withPartitionKey("tid", DataTypes.TEXT)
                .withColumn("write_set", DataTypes.setOf(DataTypes.TEXT));
        Insert txnLockWriteBuilder = QueryBuilder.insertInto("ramp", "txn_lock")
                .value("key", QueryBuilder.bindMarker())
                .value("ts", QueryBuilder.bindMarker())
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

    public void read(CqlInfo cqlInfo) {
        Select updatedByTxn = QueryBuilder.selectFrom("ramp", "txn_lock").all().whereColumn("key").in(cqlInfo.getKeys());
        ResultSet resultSet = session.execute(updatedByTxn.build());
    }
}
