package org.example;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.apache.commons.net.ntp.TimeStamp;
import org.example.common.CqlInfo;
import org.example.common.CqlType;
import org.example.db.DBStrategy;
import org.example.db.impl.CassandraImpl;
import org.example.utils.CqlParser;
import org.example.utils.SnowflakeDistributeId;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Instant;
import java.util.*;

public class Main {

    private static DBStrategy dbStrategy;
    private static SnowflakeDistributeId snowflakeDistributeId;
    static {
        Properties props = new Properties();
        try {
            props.load(SnowflakeDistributeId.class.getResourceAsStream("/snowflake.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        long workerId = Integer.parseInt(props.getProperty("workerId"));
        long datacenterId = Integer.parseInt(props.getProperty("datacenterId"));
        snowflakeDistributeId = new SnowflakeDistributeId(workerId, datacenterId);
    }

    public static void main(String[] args) {
        dbStrategy = new CassandraImpl();
        Scanner sc = new Scanner(System.in);
        String command;
        System.out.print("> ");
        String nextLine = sc.nextLine();
        StringBuilder sb = new StringBuilder();
        List<CqlInfo> txnCqls = null;
        while (!nextLine.matches("exit;?")) {
            sb.append(nextLine);
            if (nextLine.endsWith(";")) {
                command = sb.toString();
                sb.delete(0, sb.length());
                if (command.matches("begin[\\s\\n\\r]+ramp[\\s\\n\\r]*;")) {
                    txnCqls = new LinkedList<>();
                } else if (command.matches("end[\\s\\n\\r]+ramp[\\s\\n\\r]*;")) {
                    if (txnCqls.get(0).getType() == CqlType.SELECT) {
                        try {
                            Collection<String> result = dbStrategy.txnRead(txnCqls);
                            for (String s : result) {
                                System.out.println(s);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else {
                        long tid = snowflakeDistributeId.nextId();
                        Instant timestamp = getNTPTime();
                        try {
                            dbStrategy.txnWrite(txnCqls, tid, timestamp);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    txnCqls = null;
                } else if (txnCqls != null) {
                    txnCqls.add(CqlParser.parse(command));
                } else {
                    try {
                        ResultSet resultSet = dbStrategy.nonTxn(command);
                        for (Row row : resultSet) {
                            System.out.println(row.getFormattedContents());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.print("> ");
            nextLine = sc.nextLine();
        }
        sc.close();
        System.out.println("Bye");
        System.exit(0);
    }

    private static Instant getNTPTime() {
        NTPUDPClient timeClient = new NTPUDPClient();
        String timeServerUrl = "ntp.aliyun.com";
        TimeInfo timeInfo = null;
        try {
            InetAddress timeServerAddress = InetAddress.getByName(timeServerUrl);
            timeInfo = timeClient.getTime(timeServerAddress);
        } catch (Exception e) {
            e.printStackTrace();
        }
        TimeStamp timeStamp = timeInfo.getMessage().getTransmitTimeStamp();
        return Instant.ofEpochMilli(timeStamp.getTime());
    }
}