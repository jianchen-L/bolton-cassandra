package org.example.db.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import org.example.common.CqlInfo;
import org.example.common.CqlType;
import org.example.db.DBStrategy;
import org.example.utils.SnowflakeDistributeId;

import java.io.IOException;
import java.util.*;

public class CassandraImpl implements DBStrategy {

    private static SnowflakeDistributeId snowflakeDistributeId;
    private final CqlSession session;
    private final PreparedStatement preparedInsertTxnInfo;
    private final PreparedStatement preparedSelectTxnInfo;

    public CassandraImpl() {
        Properties props = new Properties();
        try {
            props.load(SnowflakeDistributeId.class.getResourceAsStream("/snowflake.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        long workerId = Integer.parseInt(props.getProperty("workerId"));
        long datacenterId = Integer.parseInt(props.getProperty("datacenterId"));
        snowflakeDistributeId = new SnowflakeDistributeId(workerId, datacenterId);

        session = CqlSession.builder().build();
        CreateKeyspace createRamp = SchemaBuilder.createKeyspace("ramp").ifNotExists().withSimpleStrategy(3);
        session.execute(createRamp.build());
        CreateTable createTxnInfo = SchemaBuilder.createTable("ramp", "txn_info").ifNotExists()
                .withPartitionKey("tid", DataTypes.BIGINT)
                .withColumn("write_set", DataTypes.TEXT);
        session.execute(createTxnInfo.build());
        Insert insertTxnInfo = QueryBuilder.insertInto("ramp", "txn_info")
                .value("tid", QueryBuilder.bindMarker())
                .value("write_set", QueryBuilder.bindMarker());
        preparedInsertTxnInfo = session.prepare(insertTxnInfo.build());
        Select selectTxnInfo = QueryBuilder.selectFrom("ramp", "txn_info")
                .columns("tid", "write_set").selector(Selector.writeTime("write_set")).as("info_ts").whereColumn("tid").isEqualTo(QueryBuilder.bindMarker());
        preparedSelectTxnInfo = session.prepare(selectTxnInfo.build());
    }

    @Override
    public ResultSet nonTxn(String cql) {
        return session.execute(cql);
    }

    @Override
    public Collection<Row> txnRead(List<CqlInfo> cqlInfos) {
        // 关联读取结果的key和值
        Map<String, Row> result = new HashMap<>();
        Map<String, Row> allKeys = new HashMap<>();
        Map<String, CqlInfo> keyCqlInfoMap = new HashMap<>();
        List<Row> updatedByTxnList = new LinkedList<>();
        for (CqlInfo cqlInfo : cqlInfos) {
            allKeys.putAll(cqlInfo.getKeys());
            Select selectResult = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns()).columns("key", "tid").selector(Selector.writeTime("tid")).as("lock_ts");
            for (ColumnMetadata primaryColumn : cqlInfo.getTableMetadata().getPrimaryKey()) {
                selectResult = selectResult.whereColumn(primaryColumn.getName()).isEqualTo(QueryBuilder.bindMarker());
            }
            PreparedStatement preparedSelectResult = session.prepare(selectResult.build());
            if (cqlInfo.getTableMetadata().getColumn("key").isPresent()) {
                for (Map.Entry<String, Row> key : cqlInfo.getKeys().entrySet()) {
                    keyCqlInfoMap.put(key.getKey(), cqlInfo);
                    BoundStatementBuilder boundSelectResult = preparedSelectResult.boundStatementBuilder();
                    Row keyRow = key.getValue();
                    for (int i = 0; i < keyRow.size(); i++) {
                        TypeCodec<Object> codec = boundSelectResult.codecRegistry().codecFor(keyRow.getType(i));
                        boundSelectResult.set(i, keyRow.get(i, codec), codec);
                    }
                    result.put(key.getKey(), session.execute(boundSelectResult.build()).one());
                    if (result.get(key.getKey()).getString("key") != null) {
                        updatedByTxnList.add(result.get(key.getKey()));
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
                    result.put(key.getKey(), session.execute(boundSelectResult.build()).one());
                }
            }
        }
        // 原子可见性检查
        // 检查被事务更新过的数据项
        if (!updatedByTxnList.isEmpty()) {
            while (!session.isClosed()) {
                // 从事务元数据获取数据项的最新时间戳
                Map<String, Long> latest = new HashMap<>(updatedByTxnList.size());
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
                    String[] writeSet = txnInfo.getString("write_set").split(",");
                    for (String key : writeSet) {
                        long ts = txnInfo.getLong("info_ts");
                        if (latest.getOrDefault(key, 0L) < ts) {
                            latest.put(key, ts);
                        }
                    }
                }
                // 获取需要等待复制的数据项
                List<String> needed = new LinkedList<>();
                for (Row resultRow : result.values()) {
                    String key = resultRow.getString("key");
                    long latestOfKey = latest.get(key);
                    if (latestOfKey > resultRow.getLong("lock_ts")) {
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
//                    System.out.println("尝试获取最新版本第" + loopCount + "次");
                    for (String neededKey : needed) {
                        CqlInfo cqlInfo = keyCqlInfoMap.get(neededKey);
                        Row keyRow = allKeys.get(neededKey);
                        List<ColumnMetadata> primaryColumns = cqlInfo.getTableMetadata().getPrimaryKey();
                        Select selectNeeded = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns()).selector(Selector.writeTime("tid")).as("lock_ts");
                        for (int i = 0; i < keyRow.size(); i++) {
                            TypeCodec<Object> codec = keyRow.codecRegistry().codecFor(keyRow.getType(i));
                            selectNeeded = selectNeeded.whereColumn(primaryColumns.get(i).getName())
                                    .isEqualTo(QueryBuilder.literal(keyRow.get(i, codec)));
                        }
                        Row neededRow = session.execute(selectNeeded.build()).one();
                        long ts = neededRow.getLong("lock_ts");
                        int compare = latest.get(neededKey).compareTo(ts);
                        if (compare == 0) {
                            needed.remove(neededKey);
                            result.put(neededKey, neededRow);
                        } else if (compare < 0) {
                            needed.remove(neededKey);
                            newers.add(neededKey);
                        }
                    }
                }
                if (newers.size() == 0) {
                    break;
                } else {
//                    System.out.println("读取到更新版本，重新进行原子可见性检查");
                    updatedByTxnList.clear();
                    for (String newer : newers) {
                        CqlInfo cqlInfo = keyCqlInfoMap.get(newer);
                        Row keyRow = allKeys.get(newer);
                        Select selectUpdatedByTxn = QueryBuilder.selectFrom(cqlInfo.getKeyspace(), cqlInfo.getTable()).raw(cqlInfo.getSelectColumns()).columns("key", "tid").selector(Selector.writeTime("tid")).as("lock_ts");
                        List<ColumnMetadata> primaryColumns = cqlInfo.getTableMetadata().getPrimaryKey();
                        for (int i = 0; i < keyRow.size(); i++) {
                            TypeCodec<Object> codec = keyRow.codecRegistry().codecFor(keyRow.getType(i));
                            selectUpdatedByTxn = selectUpdatedByTxn.whereColumn(primaryColumns.get(i).getName())
                                    .isEqualTo(QueryBuilder.literal(keyRow.get(i, codec)));
                        }
                        result.put(newer, session.execute(selectUpdatedByTxn.build()).one());
                        updatedByTxnList.add(result.get(newer));
                    }
                }
            }
        }
        return result.values();
    }

    @Override
    public void txnWrite(List<CqlInfo> cqlInfos) {
        long tid = snowflakeDistributeId.nextId();
        BatchStatementBuilder batchStatementBuilder = BatchStatement.builder(DefaultBatchType.LOGGED);
        Set<String> writeSet = new HashSet<>();
        for (CqlInfo cqlInfo : cqlInfos) {
            writeSet.addAll(cqlInfo.getKeys().keySet());
            if (cqlInfo.getTableMetadata().getColumn("key").isEmpty()) {
                try {
                    session.execute(SchemaBuilder.alterTable(cqlInfo.getKeyspace(), cqlInfo.getTable()).addColumn("key", DataTypes.TEXT).addColumn("tid", DataTypes.BIGINT).build());
                } catch (InvalidQueryException ignored) {

                }
            }
            if (cqlInfo.getType() == CqlType.INSERT) {
                Map<String, Term> insertKVCast = new HashMap<>();
                for (Map.Entry<String, String> insertKV : cqlInfo.getWriteValues().entrySet()) {
                    insertKVCast.put(insertKV.getKey(), QueryBuilder.literal(insertKV.getValue()));
                }
                RegularInsert insert = QueryBuilder.insertInto(cqlInfo.getKeyspace(), cqlInfo.getTable())
                        .value("key", QueryBuilder.literal(cqlInfo.getKeys().keySet().iterator().next()))
                        .value("tid", QueryBuilder.literal(tid));
                for (Map.Entry<String, String> insertKV : cqlInfo.getWriteValues().entrySet()) {
                    ColumnMetadata columnMetadata = cqlInfo.getTableMetadata().getColumn(insertKV.getKey()).get();
                    TypeCodec<Object> codec = session.getContext().getCodecRegistry().codecFor(columnMetadata.getType());
                    insert = insert.value(insertKV.getKey(), QueryBuilder.literal(codec.parse(insertKV.getValue())));
                }
                batchStatementBuilder.addStatement(insert.build());
            } else if (cqlInfo.getType() == CqlType.UPDATE) {
                UpdateWithAssignments updateWithAssignments = QueryBuilder.update(cqlInfo.getKeyspace(), cqlInfo.getTable())
                        .set(Assignment.setColumn("tid", QueryBuilder.literal(tid)));
                for (Map.Entry<String, String> updateKV : cqlInfo.getWriteValues().entrySet()) {
                    ColumnMetadata columnMetadata = cqlInfo.getTableMetadata().getColumn(updateKV.getKey()).get();
                    TypeCodec<Object> codec = session.getContext().getCodecRegistry().codecFor(columnMetadata.getType());
                    updateWithAssignments = updateWithAssignments.setColumn(updateKV.getKey(), QueryBuilder.literal(codec.parse(updateKV.getValue())));
                }
                batchStatementBuilder.addStatement(updateWithAssignments.whereRaw(cqlInfo.getRaw().substring(cqlInfo.getRaw().indexOf("WHERE") + 5, cqlInfo.getRaw().length() - 1)).build());
            } else {
                batchStatementBuilder.addStatement(SimpleStatement.newInstance(cqlInfo.getRaw()));
            }
        }
        batchStatementBuilder.addStatement(preparedInsertTxnInfo.bind(tid, String.join(",", writeSet)));
        session.execute(batchStatementBuilder.build());
    }
}
