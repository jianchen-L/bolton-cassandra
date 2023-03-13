package org.example.db.impl;

import com.datastax.oss.driver.api.core.cql.ResultSet;
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
        Collection<String> result = cassandra.txnRead(cqlInfos);
        for (String str : result) {
            System.out.println(str);
        }
    }

    @Test
    void nonTxn() {
        ResultSet resultSet = cassandra.nonTxn("INSERT INTO tutorialspoint.emp (emp_name, emp_id, emp_city, " +
                "emp_phone, emp_sal) VALUES('yuhan', 7,'chengdu', 9848022337, 44000);");
        System.out.println(resultSet.wasApplied());
    }

    @Test
    void txnWrite() {
        List<CqlInfo> cqlInfos = new LinkedList<>();
        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_name, emp_id, emp_city, " +
                "emp_phone, emp_sal) VALUES('jason', 4,'chengdu', 9848022337, 44000);"));
        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
                "emp_phone, emp_sal) VALUES(5,'jay', 'beijing', 9848022336, 47000);"))
        ;
        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
                "emp_phone, emp_sal) VALUES(6,'bob', 'shanghai', 9848022335, 42000);"));
//        cqlInfos.add(CqlParser.parse("UPDATE tutorialspoint.emp SET age = 28 WHERE lastname = 'WELTEN' and firstname = 'Bram' IF EXISTS;"));
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
        cassandra.nonTxn("INSERT INTO correct.test_a (id, update_text) VALUES (0,'none');");
        cassandra.nonTxn("INSERT INTO correct.test_b (id, update_text) VALUES (0,'none');");
        ThreadPoolExecutor writer = new ThreadPoolExecutor(5, 10, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(100), new ThreadPoolExecutor.DiscardPolicy());
        for (int i = 0; i < 5; i++) {
            writer.execute(()->{
                DBStrategy dbStrategy = new CassandraImpl();
                List<CqlInfo> writeCqls = new LinkedList<>();
                for (int j = 0; j < 100; j++) {
                    writeCqls.clear();
                    writeCqls.add(CqlParser.parse(String.format("UPDATE correct.test_a SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName())));
                    writeCqls.add(CqlParser.parse(String.format("UPDATE correct.test_b SET update_text='%s' WHERE id = 0;", j + Thread.currentThread().getName())));
                    dbStrategy.txnWrite(writeCqls);
                    try {
                        Thread.sleep(1000);
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
        Collection<String> result;
        Iterator<String> iterator;
        String a, b;
        int right = 0, fail = 0;
        while (!writer.isTerminated()) {
            result = readDb.txnRead(readCqls);
            iterator = result.iterator();
            a = iterator.next();
            b = iterator.next();
            if (a.equals(b)) {
                right++;
            } else {
                System.out.println(a + ", " + b);
                fail++;
            }
            Thread.sleep(10);
        }
        System.out.println("right = " + right + ", wrong = " + fail);
    }
}