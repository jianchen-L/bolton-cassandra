package org.example.db.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.example.common.CqlInfo;
import org.example.db.DBStrategy;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class CassandraImpl implements DBStrategy {

    private final CqlSession session;
    private final PreparedStatement preparedInsertTxnInfo;
    private final PreparedStatement preparedSelectTxnInfo;

    public CassandraImpl() {
        session = CqlSession.builder().build();
        CreateKeyspace createRamp = SchemaBuilder.createKeyspace("ramp").ifNotExists().withSimpleStrategy(3);
        session.execute(createRamp.build());
        CreateTable createTxnInfo = SchemaBuilder.createTable("ramp", "txn_info").ifNotExists()
                .withPartitionKey("tid", DataTypes.BIGINT)
                .withColumn("info_ts", DataTypes.TIMESTAMP)
                .withColumn("write_set", DataTypes.setOf(DataTypes.TEXT));
        session.execute(createTxnInfo.build());
        Insert insertTxnInfo = QueryBuilder.insertInto("ramp", "txn_info")
                .value("tid", QueryBuilder.bindMarker())
                .value("info_ts", QueryBuilder.bindMarker())
                .value("write_set", QueryBuilder.bindMarker());
        preparedInsertTxnInfo = session.prepare(insertTxnInfo.build());
        Select selectTxnInfo = QueryBuilder.selectFrom("ramp", "txn_info")
                .all().whereColumn("tid").isEqualTo(QueryBuilder.bindMarker());
        preparedSelectTxnInfo = session.prepare(selectTxnInfo.build());
    }

    @Override
    public Collection<String> read(List<CqlInfo> cqlInfos) {
        // 关联读取结果的key和值
        Map<String, String> result = new HashMap<>();
        Map<String, Row> allKeys = new HashMap<>();
        Map<String, CqlInfo> keyCqlInfoMap = new HashMap<>();
        List<Row> updatedByTxnList = new LinkedList<>();
        for (CqlInfo cqlInfo : cqlInfos) {
            allKeys.putAll(cqlInfo.getKeys());
            Select selectResult = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns());
            Select selectUpdatedByTxn = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), "txn_lock_" + cqlInfo.getTable()).columns("lock_ts", "tid");
            for (ColumnMetadata primaryColumn : cqlInfo.getTableMetadata().getPrimaryKey()) {
                selectResult = selectResult.whereColumn(primaryColumn.getName()).isEqualTo(QueryBuilder.bindMarker());
                selectUpdatedByTxn = selectUpdatedByTxn.whereColumn(primaryColumn.getName()).isEqualTo(QueryBuilder.bindMarker());
            }
            PreparedStatement preparedSelectResult = session.prepare(selectResult.build());
            PreparedStatement preparedSelectUpdatedByTxn = session.prepare(selectUpdatedByTxn.build());
            for (Map.Entry<String, Row> key : cqlInfo.getKeys().entrySet()) {
                keyCqlInfoMap.put(key.getKey(), cqlInfo);
                BoundStatementBuilder boundSelectResult = preparedSelectResult.boundStatementBuilder();
                BoundStatementBuilder boundSelectUpdatedByTxn = preparedSelectUpdatedByTxn.boundStatementBuilder();
                Row keyRow = key.getValue();
                for (int i = 0; i < keyRow.size(); i++) {
                    TypeCodec<Object> codec = boundSelectResult.codecRegistry().codecFor(keyRow.getType(i));
                    boundSelectResult.set(i, keyRow.get(i, codec), codec);
                    boundSelectUpdatedByTxn.set(i, keyRow.get(i, codec), codec);
                }
                result.put(key.getKey(), session.execute(boundSelectResult.build()).one().getFormattedContents());
                Row updatedByTxnRow = session.execute(boundSelectUpdatedByTxn.build()).one();
                if (updatedByTxnRow != null) {
                    updatedByTxnList.add(updatedByTxnRow);
                }
            }
        }
        // 原子可见性检查
        // 检查被事务更新过的数据项
        if (updatedByTxnList.isEmpty()) {
            return result.values();
        } else {
            while (!session.isClosed()) {
                // 从事务元数据获取数据项的最新时间戳
                Map<String, Instant> latest = new HashMap<>(updatedByTxnList.size());
                for (Row updatedByTxnRow : updatedByTxnList) {
                    // 如果txn_info更新落后于txn_lock则读不到对应元数据，循环读取
                    Row txnInfo = null;
                    // 维护一个int值来动态设置等待时间
                    int loopCount = 0;
                    while (txnInfo == null) {
                        if (loopCount++ > 0) {
                            try {
                                Thread.sleep(10L << loopCount);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        txnInfo = session.execute(preparedSelectTxnInfo.bind(updatedByTxnRow.getLong("tid"))).one();
                    }
                    Set<String> writeSet = txnInfo.getSet("write_set", String.class);
                    for (String key : writeSet) {
                        Instant ts = txnInfo.getInstant("info_ts");
                        if (latest.getOrDefault(key, Instant.MIN).isBefore(ts)) {
                            latest.put(key, ts);
                        }
                    }
                }
                // 获取需要等待复制的数据项
                List<String> needed = new LinkedList<>();
                for (Row updatedByTxnRow : updatedByTxnList) {
                    String key = updatedByTxnRow.getString("key");
                    Instant latestOfKey = latest.get(key);
                    if (latestOfKey.isAfter(updatedByTxnRow.getInstant("lock_ts"))) {
                        needed.add(key);
                    }
                }
                // 读取等待复制数据项的最新数据，如果读取到更晚的时间戳则循环读取丢失的数据项
                List<String> newers = new LinkedList<>();
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
                    for (String neededKey : needed) {
                        CqlInfo cqlInfo = keyCqlInfoMap.get(neededKey);
                        Row keyRow = allKeys.get(neededKey);
                        Select selectNeeded = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), "txn_lock_" + cqlInfo.getTable()).columns("lock_ts");
                        List<ColumnMetadata> primaryColumns = cqlInfo.getTableMetadata().getPrimaryKey();
                        for (int i = 0; i < keyRow.size(); i++) {
                            TypeCodec<Object> codec = keyRow.codecRegistry().codecFor(keyRow.getType(i));
                            selectNeeded = selectNeeded.whereColumn(primaryColumns.get(i).getName())
                                    .isEqualTo(QueryBuilder.literal(keyRow.get(i, codec)));
                        }
                        Row neededRow = session.execute(selectNeeded.build()).one();
                        Instant ts = neededRow.getInstant("lock_ts");
                        int compare = latest.get(neededKey).compareTo(ts);
                        if (compare == 0) {
                            needed.remove(neededKey);
                            Select selectResult = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns());
                            for (int i = 0; i < keyRow.size(); i++) {
                                TypeCodec<Object> codec = keyRow.codecRegistry().codecFor(keyRow.getType(i));
                                selectResult = selectResult.whereColumn(primaryColumns.get(i).getName())
                                        .isEqualTo(QueryBuilder.literal(keyRow.get(i, codec)));
                            }
                            result.put(neededKey, session.execute(selectResult.build()).one().getFormattedContents());
                        } else if (compare < 0) {
                            needed.remove(neededKey);
                            newers.add(neededKey);
                        }
                    }
                }
                if (newers.size() == 0) {
                    break;
                } else {
                    updatedByTxnList.clear();
                    for (String newer : newers) {
                        CqlInfo cqlInfo = keyCqlInfoMap.get(newer);
                        Row keyRow = allKeys.get(newer);
                        Select selectUpdatedByTxn = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), "txn_lock_" + cqlInfo.getTable()).columns("lock_ts", "tid");
                        List<ColumnMetadata> primaryColumns = cqlInfo.getTableMetadata().getPrimaryKey();
                        for (int i = 0; i < keyRow.size(); i++) {
                            TypeCodec<Object> codec = keyRow.codecRegistry().codecFor(keyRow.getType(i));
                            selectUpdatedByTxn = selectUpdatedByTxn.whereColumn(primaryColumns.get(i).getName())
                                    .isEqualTo(QueryBuilder.literal(keyRow.get(i, codec)));
                        }
                        updatedByTxnList.add(session.execute(selectUpdatedByTxn.build()).one());
                    }
                }
            }
        }
        return result.values();
    }

    @Override
    public void nonTxnWrite(CqlInfo cqlInfo) {
        session.execute(cqlInfo.getRaw());
    }

    @Override
    public CompletionStage<AsyncResultSet> txnWrite(List<CqlInfo> cqlInfos, long tid) {
        Instant timestamp = Instant.now();
        BatchStatementBuilder batchStatementBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
        Set<String> writeSet = new HashSet<>();
        for (CqlInfo cqlInfo : cqlInfos) {
            writeSet.addAll(cqlInfo.getKeys().keySet());
            CreateTable createTxnLock = null;
            CreateTableStart createTxnLockStart = SchemaBuilder.createTable(cqlInfo.getKeyspace(), "txn_lock_" + cqlInfo.getTable()).ifNotExists();
            List<ColumnMetadata> partitionColumns = cqlInfo.getTableMetadata().getPartitionKey();
            for (int i = 0; i < partitionColumns.size(); i++) {
                if (i == 0) {
                    createTxnLock = createTxnLockStart.withPartitionKey(partitionColumns.get(i).getName(), partitionColumns.get(i).getType());
                } else {
                    createTxnLock = createTxnLock.withPartitionKey(partitionColumns.get(i).getName(), partitionColumns.get(i).getType());
                }
            }
            for (ColumnMetadata clusteringColumn : cqlInfo.getTableMetadata().getClusteringColumns().keySet()) {
                createTxnLock = createTxnLock.withClusteringColumn(clusteringColumn.getName(), clusteringColumn.getType());
            }
            createTxnLock = createTxnLock.withColumn("lock_ts", DataTypes.TIMESTAMP).withColumn("tid", DataTypes.BIGINT);
            session.execute(createTxnLock.build());
            RegularInsert insertTxnLock = QueryBuilder.insertInto(cqlInfo.getKeyspace(), "txn_lock_" + cqlInfo.getTable())
                    .value("lock_ts", QueryBuilder.literal(timestamp))
                    .value("tid", QueryBuilder.literal(tid));
            for (ColumnMetadata primaryColumn : cqlInfo.getTableMetadata().getPrimaryKey()) {
                insertTxnLock = insertTxnLock.value(primaryColumn.getName(), QueryBuilder.bindMarker());
            }
            PreparedStatement preparedInsertTxnLock = session.prepare(insertTxnLock.build());
            for (Map.Entry<String, Row> key : cqlInfo.getKeys().entrySet()) {
                BoundStatementBuilder boundInsertTxnLock = preparedInsertTxnLock.boundStatementBuilder();
                Row keyRow = key.getValue();
                for (int i = 0; i < keyRow.size(); i++) {
                    TypeCodec<Object> codec = boundInsertTxnLock.codecRegistry().codecFor(keyRow.getType(i));
                    boundInsertTxnLock.set(i, keyRow.get(i, codec), codec);
                }
                batchStatementBuilder.addStatement(boundInsertTxnLock.build());
            }
            batchStatementBuilder.addStatement(SimpleStatement.newInstance(cqlInfo.getRaw()));
        }
        batchStatementBuilder.addStatement(preparedInsertTxnInfo.bind(tid, timestamp, writeSet));
        return session.executeAsync(batchStatementBuilder.build());
    }
}
