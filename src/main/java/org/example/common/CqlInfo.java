package org.example.common;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import lombok.Data;
import lombok.NonNull;

import java.util.Map;

@Data
public class CqlInfo {
    @NonNull
    private String raw;
    @NonNull
    private CqlType type;
    private String keyspace;
    private String table;
    private TableMetadata tableMetadata;
    private Map<String, Row> keys;
    private String selectColumns;
    private String[] insertValues;

    public CqlInfo(String raw) {
        this.raw = raw;
    }
}
