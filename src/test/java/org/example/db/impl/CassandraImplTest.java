package org.example.db.impl;

import org.example.utils.CqlParser;
import org.junit.jupiter.api.Test;

class CassandraImplTest {

    private CassandraImpl cassandra = new CassandraImpl();

    @Test
    void read() {
        cassandra.read(CqlParser.parse("select emp_city from tutorialspoint.emp;"));
    }
}