package org.example.db.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Metadata;
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
import org.example.common.CqlType;
import org.example.db.DBStrategy;

import java.time.Instant;
import java.util.*;

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
    public ResultSet nonTxn(String cql) {
        return session.execute(cql);
    }

    @Override
    public Collection<String> txnRead(List<CqlInfo> cqlInfos) {
        // ?????????????????????key??????
        Map<String, String> result = new HashMap<>();
        Map<String, Row> allKeys = new HashMap<>();
        Map<String, CqlInfo> keyCqlInfoMap = new HashMap<>();
        List<Row> updatedByTxnList = new LinkedList<>();
        Metadata metadata = session.getMetadata();
        for (CqlInfo cqlInfo : cqlInfos) {
            allKeys.putAll(cqlInfo.getKeys());
            Select selectResult = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns());
            Select selectUpdatedByTxn = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), "txn_lock_" + cqlInfo.getTable()).columns("key", "lock_ts", "tid");
            for (ColumnMetadata primaryColumn : cqlInfo.getTableMetadata().getPrimaryKey()) {
                selectResult = selectResult.whereColumn(primaryColumn.getName()).isEqualTo(QueryBuilder.bindMarker());
                selectUpdatedByTxn = selectUpdatedByTxn.whereColumn(primaryColumn.getName()).isEqualTo(QueryBuilder.bindMarker());
            }
            PreparedStatement preparedSelectResult = session.prepare(selectResult.build());
            boolean tableExists = metadata.getKeyspace(cqlInfo.getKeyspace()).flatMap(ks -> ks.getTable("txn_lock_" + cqlInfo.getTable())).isPresent();
            if (tableExists) {
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
            } else {
                for (Map.Entry<String, Row> key : cqlInfo.getKeys().entrySet()) {
                    keyCqlInfoMap.put(key.getKey(), cqlInfo);
                    BoundStatementBuilder boundSelectResult = preparedSelectResult.boundStatementBuilder();
                    Row keyRow = key.getValue();
                    for (int i = 0; i < keyRow.size(); i++) {
                        TypeCodec<Object> codec = boundSelectResult.codecRegistry().codecFor(keyRow.getType(i));
                        boundSelectResult.set(i, keyRow.get(i, codec), codec);
                    }
                    result.put(key.getKey(), session.execute(boundSelectResult.build()).one().getFormattedContents());
                }
            }
        }
        // ?????????????????????
        // ????????????????????????????????????
        if (!updatedByTxnList.isEmpty()) {
            while (!session.isClosed()) {
                // ???????????????????????????????????????????????????
                Map<String, Instant> latest = new HashMap<>(updatedByTxnList.size());
                for (Row updatedByTxnRow : updatedByTxnList) {
                    // ??????txn_info???????????????txn_lock??????????????????????????????????????????
                    Row txnInfo = null;
                    // ????????????int??????????????????????????????
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
                // ????????????????????????????????????
                List<String> needed = new LinkedList<>();
                for (Row updatedByTxnRow : updatedByTxnList) {
                    String key = updatedByTxnRow.getString("key");
                    Instant latestOfKey = latest.get(key);
                    if (latestOfKey.isAfter(updatedByTxnRow.getInstant("lock_ts"))) {
                        needed.add(key);
                    }
                }
                // ???????????????????????????????????????????????????????????????????????????????????????????????????????????????
                List<String> newers = new LinkedList<>();
                // ????????????int??????????????????????????????
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
    public void txnWrite(List<CqlInfo> cqlInfos, long tid, Instant timestamp) {
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
            createTxnLock = createTxnLock.withColumn("key", DataTypes.TEXT).withColumn("lock_ts", DataTypes.TIMESTAMP).withColumn("tid", DataTypes.BIGINT);
            session.execute(createTxnLock.build());
            RegularInsert insertTxnLock = QueryBuilder.insertInto(cqlInfo.getKeyspace(), "txn_lock_" + cqlInfo.getTable())
                    .value("key", QueryBuilder.bindMarker())
                    .value("lock_ts", QueryBuilder.literal(timestamp))
                    .value("tid", QueryBuilder.literal(tid));
            for (ColumnMetadata primaryColumn : cqlInfo.getTableMetadata().getPrimaryKey()) {
                insertTxnLock = insertTxnLock.value(primaryColumn.getName(), QueryBuilder.bindMarker());
            }
            PreparedStatement preparedInsertTxnLock = session.prepare(insertTxnLock.build());
            if (cqlInfo.getType() == CqlType.INSERT) {
                BoundStatementBuilder boundInsertTxnLock = preparedInsertTxnLock.boundStatementBuilder();
                boundInsertTxnLock.setString(0, cqlInfo.getKeys().keySet().iterator().next());
                List<ColumnMetadata> primaryColumns = cqlInfo.getTableMetadata().getPrimaryKey();
                Map<String, String> insertValues = cqlInfo.getInsertValues();
                for (int i = 0; i < primaryColumns.size(); i++) {
                    ColumnMetadata columnMetadata = primaryColumns.get(i);
                    TypeCodec<Object> codec = boundInsertTxnLock.codecRegistry().codecFor(columnMetadata.getType());
                    // ???0?????????????????????key?????????????????????????????????1??????????????????i + 1
                    boundInsertTxnLock.set(i + 1, codec.parse(insertValues.get(columnMetadata.getName().asInternal())), codec);
                }
                batchStatementBuilder.addStatement(boundInsertTxnLock.build());
            } else {
                for (Map.Entry<String, Row> key : cqlInfo.getKeys().entrySet()) {
                    BoundStatementBuilder boundInsertTxnLock = preparedInsertTxnLock.boundStatementBuilder();
                    boundInsertTxnLock.setString(0, key.getKey());
                    Row keyRow = key.getValue();
                    for (int i = 0; i < keyRow.size(); i++) {
                        TypeCodec<Object> codec = boundInsertTxnLock.codecRegistry().codecFor(keyRow.getType(i));
                        boundInsertTxnLock.set(i + 1, keyRow.get(i, codec), codec);
                    }
                    batchStatementBuilder.addStatement(boundInsertTxnLock.build());
                }
            }
            batchStatementBuilder.addStatement(SimpleStatement.newInstance(cqlInfo.getRaw()));
        }
        batchStatementBuilder.addStatement(preparedInsertTxnInfo.bind(tid, timestamp, writeSet));
        session.execute(batchStatementBuilder.build());
    }
}
