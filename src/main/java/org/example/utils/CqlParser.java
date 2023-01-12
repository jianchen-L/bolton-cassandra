package org.example.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.example.common.CqlInfo;
import org.example.common.CqlType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CqlParser {

    private static final CqlSession session = CqlSession.builder().build();
    private static final Pattern selectPattern = Pattern.compile("select[\\s\\n\\r]+([\\s\\S]+)[\\s\\n\\r]+from[\\s\\n\\r]+([\\w.]+)[\\s\\S]*;$", Pattern.CASE_INSENSITIVE);
    private static final Pattern insertPattern = Pattern.compile("insert[\\s\\n\\r]+into[\\s\\n\\r]+([\\w.]+)[\\s\\n\\r]*\\(([\\s\\S]+)\\)[\\s\\n\\r]*values[\\s\\n\\r]*\\(([\\s\\S]+)\\)[\\s\\S]*;$", Pattern.CASE_INSENSITIVE);
    private static final Pattern updatePattern = Pattern.compile("update[\\s\\n\\r]+([\\w.]+)[\\s\\S]+set[\\s\\S]+where[\\s\\n\\r]+([\\s\\S]+?)(if[\\s\\S]+)*;$", Pattern.CASE_INSENSITIVE);
    private static final Pattern deletePattern = Pattern.compile("delete[\\s\\n\\r]+([\\w\\s\\n\\r,]*)from[\\s\\n\\r]+([\\w.]+)[\\s\\S]+where[\\s\\n\\r]+([\\s\\S]+?)(if[\\s\\S]+)*;$", Pattern.CASE_INSENSITIVE);

    public static CqlInfo parse(String cql) {
        CqlInfo cqlInfo = new CqlInfo(cql);
        Matcher selectMatcher = selectPattern.matcher(cql);
        Matcher insertMatcher = insertPattern.matcher(cql);
        Matcher updateMatcher = updatePattern.matcher(cql);
        Matcher deleteMatcher = deletePattern.matcher(cql);
        if (selectMatcher.matches()) {
            cqlInfo.setType(CqlType.SELECT);
            // 解析出要查询的键空间和表
            String location = selectMatcher.group(2);
            if (!location.contains(".")) {
                location = session.getKeyspace().get().asInternal() + "." + location;
            }
            String[] keyspaceAndTable = location.split("\\.");
            cqlInfo.setKeyspace(keyspaceAndTable[0]);
            cqlInfo.setTable(keyspaceAndTable[1]);
            // 解析要查询的列
            cqlInfo.setSelectColumns(selectMatcher.group(1));
            // 查询主键所在属性
            List<ColumnMetadata> primaryColumns = session.getMetadata().getKeyspace(keyspaceAndTable[0]).get().getTable(keyspaceAndTable[1]).get().getPrimaryKey();
            cqlInfo.setPrimaryColumns(primaryColumns);
            // 获取查询数据的对应主键值
            StringBuilder primaryColumnsName = new StringBuilder(primaryColumns.get(0).getName().asInternal());
            for (int i = 1; i < primaryColumns.size(); i++) {
                primaryColumnsName.append(",").append(primaryColumns.get(i).getName().asInternal());
            }
            String selectPrimaryKVs = cql.replaceFirst("*".equals(selectMatcher.group(1)) ? "\\*" : selectMatcher.group(1), primaryColumnsName.toString());
            ResultSet primaryKVs = session.execute(selectPrimaryKVs);
            // 组装查询数据的key = keyspace.table.primaryKV，设置cqlInfo主键值
            Map<String, Row> keys = new HashMap<>();
            for (Row row : primaryKVs) {
                keys.put(location + "." + row.getFormattedContents(), row);
            }
            cqlInfo.setKeys(keys);
        } else if (insertMatcher.matches()) {
            cqlInfo.setType(CqlType.INSERT);
            // 解析出要插入的键空间和表
            String location = insertMatcher.group(1);
            if (!location.contains(".")) {
                location = session.getKeyspace().get().asInternal() + "." + location;
            }
            String[] keyspaceAndTable = location.split("\\.");
            cqlInfo.setKeyspace(keyspaceAndTable[0]);
            cqlInfo.setTable(keyspaceAndTable[1]);
            // 查询主键所在属性
            List<ColumnMetadata> primaryColumns = session.getMetadata().getKeyspace(keyspaceAndTable[0]).get().getTable(keyspaceAndTable[1]).get().getPrimaryKey();
            // 获取插入数据的对应主键值
            Map<String, String> kv = new HashMap<>();
            String[] names = insertMatcher.group(2).replaceAll("[\\s\\n\\r]", "").split(",");
            String[] values = insertMatcher.group(3).replaceAll("[\\s\\n\\r]", "").split(",");
            for (int i = 0; i < names.length; i++) {
                kv.put(names[i], values[i]);
            }
            StringBuilder keySB = new StringBuilder(location + "." + "[");
            for (int i = 0; i < primaryColumns.size(); i++) {
                if (i > 0) {
                    keySB.append(", ");
                }
                String name = primaryColumns.get(i).getName().asInternal();
                String value = kv.get(name);
                keySB.append(name).append(':').append(value);
            }
            keySB.append("]");
            // 设置插入数据的key
            Map<String, Row> keys = new HashMap<>();
            keys.put(keySB.toString(), null);
            cqlInfo.setKeys(keys);
        } else if (updateMatcher.matches()) {
            cqlInfo.setType(CqlType.UPDATE);
            // 解析出要插入的键空间和表
            String location = updateMatcher.group(1);
            if (!location.contains(".")) {
                location = session.getKeyspace().get().asInternal() + "." + location;
            }
            String[] keyspaceAndTable = location.split("\\.");
            cqlInfo.setKeyspace(keyspaceAndTable[0]);
            cqlInfo.setTable(keyspaceAndTable[1]);
            // 查询主键所在属性
            List<ColumnMetadata> primaryColumns = session.getMetadata().getKeyspace(keyspaceAndTable[0]).get().getTable(keyspaceAndTable[1]).get().getPrimaryKey();
            // 获取更新数据的对应主键值
            StringBuilder primaryColumnsName = new StringBuilder(primaryColumns.get(0).getName().asInternal());
            for (int i = 1; i < primaryColumns.size(); i++) {
                primaryColumnsName.append(",").append(primaryColumns.get(i).getName().asInternal());
            }
            Select selectPrimaryKVs = QueryBuilder.selectFrom(keyspaceAndTable[0], keyspaceAndTable[1]).raw(primaryColumnsName.toString()).whereRaw(updateMatcher.group(2));
            ResultSet primaryKVs = session.execute(selectPrimaryKVs.build());
            // 组装查询数据的key = keyspace.table.primaryKey
            Map<String, Row> keys = new HashMap<>();
            for (Row row : primaryKVs) {
                keys.put(location + "." + row.getFormattedContents(), row);
            }
            cqlInfo.setKeys(keys);
        } else if (deleteMatcher.matches()) {
            if (deleteMatcher.group(1).isBlank()) {
                cqlInfo.setType(CqlType.DELETE_ROW);
            } else {
                cqlInfo.setType(CqlType.DELETE_DATA);
            }
            // 解析出要插入的键空间和表
            String location = deleteMatcher.group(2);
            if (!location.contains(".")) {
                location = session.getKeyspace().get().asInternal() + "." + location;
            }
            String[] keyspaceAndTable = location.split("\\.");
            cqlInfo.setKeyspace(keyspaceAndTable[0]);
            cqlInfo.setTable(keyspaceAndTable[1]);
            // 查询主键所在属性
            List<ColumnMetadata> primaryColumns = session.getMetadata().getKeyspace(keyspaceAndTable[0]).get().getTable(keyspaceAndTable[1]).get().getPrimaryKey();
            // 获取更新数据的对应主键值
            StringBuilder primaryColumnsName = new StringBuilder(primaryColumns.get(0).getName().asInternal());
            for (int i = 1; i < primaryColumns.size(); i++) {
                primaryColumnsName.append(",").append(primaryColumns.get(i).getName().asInternal());
            }
            Select selectPrimaryKVs = QueryBuilder.selectFrom(keyspaceAndTable[0], keyspaceAndTable[1]).raw(primaryColumnsName.toString()).whereRaw(deleteMatcher.group(3));
            ResultSet primaryKVs = session.execute(selectPrimaryKVs.build());
            // 组装查询数据的key = keyspace.table.primaryKey
            Map<String, Row> keys = new HashMap<>();
            for (Row row : primaryKVs) {
                keys.put(location + "." + row.getFormattedContents(), row);
            }
            cqlInfo.setKeys(keys);
        } else {
            cqlInfo.setType(CqlType.OTHERS);
        }
        return cqlInfo;
    }
}
