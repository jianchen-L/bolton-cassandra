package org.example.db.impl;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.apache.commons.net.ntp.TimeStamp;
import org.example.common.CqlInfo;
import org.example.utils.CqlParser;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

class CassandraImplTest {

    private final CassandraImpl cassandra = new CassandraImpl();

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
                "emp_phone, emp_sal) VALUES(5,'jay', 'beijing', 9848022336, 47000);"));
        cqlInfos.add(CqlParser.parse("INSERT INTO tutorialspoint.emp (emp_id, emp_name, emp_city, " +
                "emp_phone, emp_sal) VALUES(6,'bob', 'shanghai', 9848022335, 42000);"));
//        cqlInfos.add(CqlParser.parse("UPDATE tutorialspoint.emp SET age = 28 WHERE lastname = 'WELTEN' and firstname = 'Bram' IF EXISTS;"));
//        cqlInfos.add(CqlParser.parse("DELETE FROM tutorialspoint.emp WHERE emp_id=1;"));
        NTPUDPClient timeClient = new NTPUDPClient();
        String timeServerUrl = "cn.pool.ntp.org";
        TimeInfo timeInfo = null;
        try {
            InetAddress timeServerAddress = InetAddress.getByName(timeServerUrl);
            timeInfo = timeClient.getTime(timeServerAddress);
        } catch (Exception e) {
            e.printStackTrace();
        }
        TimeStamp timeStamp = timeInfo.getMessage().getTransmitTimeStamp();
        Instant now = Instant.ofEpochSecond(timeStamp.getTime());
        System.out.println(now);
        cassandra.txnWrite(cqlInfos, 102L, Instant.now());
    }
}