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
        cassandra.nonTxn("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
        cassandra.nonTxn("DROP TABLE IF EXISTS test.table_a;");
        cassandra.nonTxn("DROP TABLE IF EXISTS test.table_b;");
        cassandra.nonTxn("CREATE TABLE test.table_a(id int PRIMARY KEY, update_text text);");
        cassandra.nonTxn("CREATE TABLE test.table_b(id int PRIMARY KEY, update_text text);");
        List<CqlInfo> insertCqls = new LinkedList<>();
        insertCqls.add(CqlParser.parse("INSERT INTO test.table_a (id, update_text) VALUES (0,'none');"));
        insertCqls.add(CqlParser.parse("INSERT INTO test.table_b (id, update_text) VALUES (0,'none');"));
        cassandra.txnWrite(insertCqls);
        Thread.sleep(1000);
        ThreadPoolExecutor testWriter = new ThreadPoolExecutor(30, 30, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.DiscardPolicy());
        for (int i = 0; i < 30; i++) {
            testWriter.execute(() -> {
                Random sleepTime = new Random();
                DBStrategy dbStrategy = new CassandraImpl();
                List<CqlInfo> updateCqls = new LinkedList<>();
                for (int j = 0; j < 100; j++) {
                    updateCqls.clear();
                    updateCqls.add(CqlParser.parse(String.format("UPDATE test.table_a SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName())));
                    updateCqls.add(CqlParser.parse(String.format("UPDATE test.table_b SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName())));
                    dbStrategy.txnWrite(updateCqls);
                    try {
                        Thread.sleep(sleepTime.nextInt(100, 300));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        testWriter.shutdown();
        List<CqlInfo> readCqls = new LinkedList<>();
        readCqls.add(CqlParser.parse("SELECT update_text FROM test.table_a WHERE id = 0;"));
        readCqls.add(CqlParser.parse("SELECT update_text FROM test.table_b WHERE id = 0;"));
        DBStrategy readDb = new CassandraImpl();
        Collection<Row> result;
        Iterator<Row> iterator;
        String a, b;
        int right = 0, fail = 0;
        while (!testWriter.isTerminated()) {
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
        System.out.println("实验组：right = " + right + ", wrong = " + fail);
        System.out.println("------------------------------------------------------------------------");
        Thread.sleep(1000);
        cassandra.nonTxn("CREATE KEYSPACE IF NOT EXISTS control WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
        cassandra.nonTxn("DROP TABLE IF EXISTS control.table_a;");
        cassandra.nonTxn("DROP TABLE IF EXISTS control.table_b;");
        cassandra.nonTxn("CREATE TABLE control.table_a(id int PRIMARY KEY, update_text text);");
        cassandra.nonTxn("CREATE TABLE control.table_b(id int PRIMARY KEY, update_text text);");
        cassandra.nonTxn("INSERT INTO control.table_a (id, update_text) VALUES (0,'none');");
        cassandra.nonTxn("INSERT INTO control.table_b (id, update_text) VALUES (0,'none');");
        Thread.sleep(1000);
        ThreadPoolExecutor controlWriter = new ThreadPoolExecutor(30, 30, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.DiscardPolicy());
        for (int i = 0; i < 30; i++) {
            controlWriter.execute(() -> {
                Random sleepTime = new Random();
                DBStrategy dbStrategy = new CassandraImpl();
                for (int j = 0; j < 100; j++) {
                    dbStrategy.nonTxn(String.format("UPDATE control.table_a SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName()));
                    dbStrategy.nonTxn(String.format("UPDATE control.table_b SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName()));
                    try {
                        Thread.sleep(sleepTime.nextInt(100, 300));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        controlWriter.shutdown();
        right = 0;
        fail = 0;
        while (!controlWriter.isTerminated()) {
            a = cassandra.nonTxn("SELECT update_text FROM control.table_a WHERE id = 0;").one().getString("update_text");
            b = cassandra.nonTxn("SELECT update_text FROM control.table_b WHERE id = 0;").one().getString("update_text");
            if (a.equals(b)) {
                right++;
            } else {
                System.out.println("断裂读取，a = " + a + ", b = " + b);
                fail++;
            }
            Thread.sleep(20);
        }
        System.out.println("对照组：right = " + right + ", wrong = " + fail);
    }
}