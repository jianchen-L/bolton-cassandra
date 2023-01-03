package org.example.common;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import lombok.Data;
import lombok.NonNull;

import java.util.List;
import java.util.Map;

@Data
public class CqlInfo {
    @NonNull
    private String raw;
    @NonNull
    private CqlType type;
    @NonNull
    private String keyspace;
    @NonNull
    private String table;
    @NonNull
    private List<ColumnMetadata> primaryColumns;
    @NonNull
    private Map<String, Row> keys;
    private String selectColumns;

    public CqlInfo(String raw) {
        this.raw = raw;
    }
}
