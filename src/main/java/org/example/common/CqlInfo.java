package org.example.common;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import lombok.Data;

import java.util.Set;

@Data
public class CqlInfo {
    private CqlType type;
    private Set<Term> keys;
    private String raw;

    public CqlInfo(String raw) {
        this.raw = raw;
    }
}
