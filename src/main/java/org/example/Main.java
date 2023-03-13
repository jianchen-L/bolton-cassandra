package org.example;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.example.common.CqlInfo;
import org.example.common.CqlType;
import org.example.db.DBStrategy;
import org.example.db.impl.CassandraImpl;
import org.example.utils.CqlParser;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class Main {

    private static DBStrategy dbStrategy;

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
                        try {
                            dbStrategy.txnWrite(txnCqls);
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

//    public static Instant getNTPTime() {
//        NTPUDPClient timeClient = new NTPUDPClient();
//        String timeServerUrl = "ntp.aliyun.com";
//        TimeInfo timeInfo = null;
//        try {
//            InetAddress timeServerAddress = InetAddress.getByName(timeServerUrl);
//            timeInfo = timeClient.getTime(timeServerAddress);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        TimeStamp timeStamp = timeInfo.getMessage().getTransmitTimeStamp();
//        return Instant.ofEpochMilli(timeStamp.getTime());
//    }
}