package org.example.db.impl;

import org.example.common.CqlInfo;
import org.example.utils.CqlParser;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

class CassandraImplTest {

    private CassandraImpl cassandra = new CassandraImpl();

    @Test
    void read() {
        List<CqlInfo> cqlInfos = new LinkedList<>();
        cqlInfos.add(CqlParser.parse("select emp_city from tutorialspoint.emp;"));
        cassandra.read(cqlInfos);
    }

    @Test
    void txnWrite() {
        List<CqlInfo> cqlInfos = new LinkedList<>();
        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
                "emp_phone, emp_sal) VALUES(4,'jason', 'chengdu', 9848022337, 44000);"));
        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
                "emp_phone, emp_sal) VALUES(5,'jay', 'beijing', 9848022336, 47000);"));
        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
                "emp_phone, emp_sal) VALUES(6,'bob', 'shanghai', 9848022335, 42000);"));
//        cqlInfos.add(CqlParser.parse("UPDATE tutorialspoint.emp SET age = 28 WHERE lastname = 'WELTEN' and firstname = 'Bram' IF EXISTS;"));
//        cqlInfos.add(CqlParser.parse("DELETE FROM tutorialspoint.emp WHERE emp_id=1;"));
        cassandra.txnWrite(cqlInfos, 102L);
    }
}