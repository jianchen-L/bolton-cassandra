package org.example.db.impl;

import com.datastax.oss.driver.api.core.cql.Row;
import org.example.common.CqlInfo;
import org.example.db.DBStrategy;
import org.example.utils.CqlParser;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class CassandraImplTest {

    private final DBStrategy cassandra = new CassandraImpl();

    @Test
    void read() {
        List<CqlInfo> cqlInfos = new LinkedList<>();
        cqlInfos.add(CqlParser.parse("select emp_city from tutorialspoint.emp;"));
        Collection<Row> result = cassandra.txnRead(cqlInfos);
        for (Row str : result) {
            System.out.println(str.getFormattedContents());
        }
    }

    @Test
    void nonTxn() {
//        ResultSet resultSet = cassandra.nonTxn("INSERT INTO tutorialspoint.emp (emp_name, emp_id, emp_city, " +
//                "emp_phone, emp_sal) VALUES('yuhan', 7,'chengdu', 9848022337, 44000);");
//        System.out.println(resultSet.wasApplied());
        Map<String, Row> map = new HashMap<>();
        map.put("5", cassandra.nonTxn("SELECT key from tutorialspoint.emp WHERE emp_id = 7").one());
        System.out.println(map.get("5").getString("key"));
    }

    @Test
    void txnWrite() {
        List<CqlInfo> cqlInfos = new LinkedList<>();
//        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_name, emp_id, emp_city, " +
//                "emp_phone, emp_sal) VALUES('jason', 4,'chengdu', 9848022337, 44000);"));
//        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
//                "emp_phone, emp_sal) VALUES(5,'jay', 'beijing', 9848022336, 47000);"))
//        ;
//        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
//                "emp_phone, emp_sal) VALUES(6,'bob', 'shanghai', 9848022335, 42000);"));
        cqlInfos.add(CqlParser.parse("UPDATE tutorialspoint.emp SET emp_city='Delhi',emp_sal=50000 WHERE emp_id=4;"));
//        cqlInfos.add(CqlParser.parse("DELETE FROM tutorialspoint.emp WHERE emp_id=1;"));
        cassandra.txnWrite(cqlInfos);
    }

    @Test
    void correct() throws InterruptedException {
        cassandra.nonTxn("CREATE KEYSPACE IF NOT EXISTS correct WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
        cassandra.nonTxn("DROP TABLE IF EXISTS correct.test_a;");
        cassandra.nonTxn("DROP TABLE IF EXISTS correct.test_b;");
        cassandra.nonTxn("CREATE TABLE correct.test_a(id int PRIMARY KEY, update_text text);");
        cassandra.nonTxn("CREATE TABLE correct.test_b(id int PRIMARY KEY, update_text text);");
        List<CqlInfo> insertCqls = new LinkedList<>();
        insertCqls.add(CqlParser.parse("INSERT INTO correct.test_a (id, update_text) VALUES (0,'none');"));
        insertCqls.add(CqlParser.parse("INSERT INTO correct.test_b (id, update_text) VALUES (0,'none');"));
        cassandra.txnWrite(insertCqls);
        Thread.sleep(1000);
        ThreadPoolExecutor writer = new ThreadPoolExecutor(10, 10, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.DiscardPolicy());
        for (int i = 0; i < 30; i++) {
            writer.execute(()->{
                Random sleep = new Random();
                DBStrategy dbStrategy = new CassandraImpl();
                List<CqlInfo> updateCqls = new LinkedList<>();
                for (int j = 0; j < 100; j++) {
                    updateCqls.clear();
                    updateCqls.add(CqlParser.parse(String.format("UPDATE correct.test_a SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName())));
                    updateCqls.add(CqlParser.parse(String.format("UPDATE correct.test_b SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName())));
                    dbStrategy.txnWrite(updateCqls);
                    try {
                        Thread.sleep(sleep.nextInt(100, 300));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        writer.shutdown();
        List<CqlInfo> readCqls = new LinkedList<>();
        readCqls.add(CqlParser.parse("SELECT update_text FROM correct.test_a WHERE id = 0;"));
        readCqls.add(CqlParser.parse("SELECT update_text FROM correct.test_b WHERE id = 0;"));
        DBStrategy readDb = new CassandraImpl();
        Collection<Row> result;
        Iterator<Row> iterator;
        String a, b;
        int right = 0, fail = 0;
        while (!writer.isTerminated()) {
            result = readDb.txnRead(readCqls);
            iterator = result.iterator();
            a = iterator.next().getString("update_text");
            b = iterator.next().getString("update_text");
            if (a.equals(b)) {
                right++;
            } else {
                System.out.println(a + ", " + b);
                fail++;
            }
            Thread.sleep(20);
        }
        System.out.println("right = " + right + ", wrong = " + fail);
    }
}