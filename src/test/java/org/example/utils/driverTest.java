package org.example.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.Test;


public class driverTest {

    private static CqlSession session = CqlSession.builder().build();

    @Test
    public void test() {
        ResultSet result = session.execute("UPDATE tutorialspoint.emp SET emp_city='Delhi',emp_sal=50000\n" +
                "   WHERE emp_id=2 and emp_name='robin';");
        for (Row row : result) {
            System.out.println(row.getFormattedContents());
        }
    }
}
