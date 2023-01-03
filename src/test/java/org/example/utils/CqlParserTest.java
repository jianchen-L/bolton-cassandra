package org.example.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Test;

class CqlParserTest {

    @Test
    void test() {
        CqlSession s = CqlSession.builder().build();
    }

    @Test
    void parse() {
        CqlParser.parse("select * from tutorialspoint.emp where emp_sal=50000 ALLOW FILTERING;");
//        CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city,\n" +
//                "   emp_phone, emp_sal) VALUES(1,'ram', 'Hyderabad', 9848022338, 50000);");
//        CqlParser.parse("UPDATE tutorialspoint.emp SET age = 28 WHERE lastname = 'WELTEN' and firstname = 'Bram' IF EXISTS;");
//        CqlParser.parse("DELETE FROM tutorialspoint.emp WHERE emp_id=1;");
    }
}