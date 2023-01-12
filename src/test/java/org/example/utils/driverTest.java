package org.example.utils;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;


public class driverTest {

    private static CqlSession session = CqlSession.builder().build();

    @Test
    public void insertMeta() {
        Insert txnLockWriteBuilder = QueryBuilder.insertInto("ramp", "txn_lock")
                .value("key", QueryBuilder.bindMarker())
                .value("lock_ts", QueryBuilder.bindMarker())
                .value("tid", QueryBuilder.bindMarker());
        PreparedStatement txnLockWrite = session.prepare(txnLockWriteBuilder.build());
        Instant instant = Instant.now();
        session.execute(txnLockWrite.bind("tutorialspoint.emp.[emp_id:1, emp_name:'ram']", instant, 101L));
        session.execute(txnLockWrite.bind("tutorialspoint.emp.[emp_id:2, emp_name:'robin']", instant, 101L));
        Insert insertTxnInfo = QueryBuilder.insertInto("ramp", "txn_info")
                .value("tid", QueryBuilder.literal(101L))
                .value("info_ts", QueryBuilder.literal(instant))
                .value("write_set", QueryBuilder.literal(Set.of("tutorialspoint.emp.[emp_id:1, emp_name:'ram']", "tutorialspoint.emp.[emp_id:2, emp_name:'robin']")));
        session.execute(insertTxnInfo.build());
    }

    @Test
    public void test() {
        System.out.println(session.isClosed());
    }
}
