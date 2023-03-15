package org.example;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.example.common.CqlInfo;
import org.example.db.DBStrategy;
import org.example.db.impl.CassandraImpl;
import org.example.utils.CqlParser;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class RAMPLauncher extends AbstractJavaSamplerClient {
    private DBStrategy dbStrategy;

    @Override
    /**
     * JMeter界面中展示出此方法所设置的默认参数。
     * @return
     */ public Arguments getDefaultParameters() {

        Arguments args = new Arguments();

        args.addArgument("cqlFile", "d:/temp/cqlFile.txt");

        return args;
    }

    /**
     * 执行runTest()方法前会调用此方法,可放一些初始化代码
     */
    @Override
    public void setupTest(JavaSamplerContext context) {
        dbStrategy = new CassandraImpl();

//        dbStrategy.nonTxn("CREATE KEYSPACE IF NOT EXISTS store WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};");
//        dbStrategy.nonTxn("DROP TABLE IF EXISTS store.books_by_title;");
//        dbStrategy.nonTxn("CREATE TABLE store.books_by_title(title text, author_id uuid, pages int, year int, PRIMARY KEY (title));");
    }

//    @Override
//    public SampleResult runTest(JavaSamplerContext context) {
//        // 创建SampleResult对象，用于记录执行结果的状态，并返回
//        SampleResult sampleResult = new SampleResult();
//
//        String path = context.getParameter("cqlFile");
//
//        // 开始
//        sampleResult.sampleStart();
//
//        try {
//            BufferedReader cqlFile = new BufferedReader(new FileReader(path, StandardCharsets.UTF_8));
//            String command;
//            List<CqlInfo> txnCqls = null;
//            while ((command = cqlFile.readLine()) != null) {
//                if (command.matches("begin[\\s\\n\\r]+ramp[\\s\\n\\r]*;")) {
//                    txnCqls = new LinkedList<>();
//                } else if (command.matches("end[\\s\\n\\r]+ramp[\\s\\n\\r]*;")) {
//                    if (txnCqls.get(0).getType() == CqlType.SELECT) {
//                        try {
//                            Collection<String> result = dbStrategy.txnRead(txnCqls);
//                            for (String s : result) {
//                                System.out.println(s);
//                            }
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    } else {
//                        long tid = snowflakeDistributeId.nextId();
//                        try {
//                            dbStrategy.txnWrite(txnCqls, tid);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                    txnCqls = null;
//                } else if (txnCqls != null) {
//                    txnCqls.add(CqlParser.parse(command));
//                } else {
//                    ResultSet resultSet = dbStrategy.nonTxn(command);
//                    for (Row row : resultSet) {
//                        System.out.println(row.getFormattedContents());
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        // 暂停
//        // sampleResult.samplePause();
//
//        // 重启
//        // sampleResult.sampleResume();
//
//        // 结束
//        sampleResult.sampleEnd();
//
//        // 返回
//        return sampleResult;
//    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        int threadNum = javaSamplerContext.getJMeterContext().getThreadNum();
        Random pages = new Random();
        Random year = new Random();
        Random update = new Random();
        // 创建SampleResult对象，用于记录执行结果的状态，并返回
        SampleResult sampleResult = new SampleResult();
        // 开始
        sampleResult.sampleStart();
        try {
            for (int i = 1; i <= 1000; i++) {
                if (i % 33 != 0) {
                    dbStrategy.nonTxn(String.format("INSERT INTO store.books_by_title (title, author_id, pages, year)VALUES ('%d Book %d', %s, %d, %d);", threadNum, i, UUID.randomUUID(), pages.nextInt(100, 999), year.nextInt(1900, 2023)));
                } else if (i % 5 != 0) {
                    List<CqlInfo> txnCqls = new LinkedList<>();
                    txnCqls.add(CqlParser.parse(String.format("INSERT INTO store.books_by_title (title, author_id, pages, year)VALUES ('%d Book1 Batch %d', %s, %d, %d);", threadNum, i, UUID.randomUUID(), pages.nextInt(100, 999), year.nextInt(1900, 2023))));
                    txnCqls.add(CqlParser.parse(String.format("INSERT INTO store.books_by_title (title, author_id, pages, year)VALUES ('%d Book2 Batch %d', %s, %d, %d);", threadNum, i, UUID.randomUUID(), pages.nextInt(100, 999), year.nextInt(1900, 2023))));
                    txnCqls.add(CqlParser.parse(String.format("INSERT INTO store.books_by_title (title, author_id, pages, year)VALUES ('%d Book3 Batch %d', %s, %d, %d);", threadNum, i, UUID.randomUUID(), pages.nextInt(100, 999), year.nextInt(1900, 2023))));
                    dbStrategy.txnWrite(txnCqls);
                } else {
                    List<CqlInfo> txnCqls = new LinkedList<>();
                    int num = i - 33 * update.nextInt(1, i / 33);
                    txnCqls.add(CqlParser.parse(String.format("UPDATE store.books_by_title SET pages=%d, year=%d WHERE title='%d Book1 Batch %d';", pages.nextInt(100, 999), year.nextInt(1900, 2023), threadNum, num)));
                    txnCqls.add(CqlParser.parse(String.format("UPDATE store.books_by_title SET pages=%d, year=%d WHERE title='%d Book2 Batch %d';", pages.nextInt(100, 999), year.nextInt(1900, 2023), threadNum, num)));
                    txnCqls.add(CqlParser.parse(String.format("UPDATE store.books_by_title SET pages=%d, year=%d WHERE title='%d Book3 Batch %d';", pages.nextInt(100, 999), year.nextInt(1900, 2023), threadNum, num)));
                    dbStrategy.txnWrite(txnCqls);
                }
                if (i % 3 == 0) {
                    dbStrategy.nonTxn(String.format("SELECT title, author_id FROM store.books_by_title WHERE title = '%d Book %d';", threadNum, i / 2));
                }
                if (i % 99 == 0) {
                    List<CqlInfo> txnCqls = new LinkedList<>();
                    txnCqls.add(CqlParser.parse(String.format("SELECT title, author_id FROM store.books_by_title WHERE title = '%d Book1 Batch %d';", threadNum, i / 3)));
                    txnCqls.add(CqlParser.parse(String.format("SELECT title, author_id FROM store.books_by_title WHERE title = '%d Book2 Batch %d';", threadNum, i / 3)));
                    txnCqls.add(CqlParser.parse(String.format("SELECT title, author_id FROM store.books_by_title WHERE title = '%d Book3 Batch %d';", threadNum, i / 3)));
                    dbStrategy.txnRead(txnCqls);
                }
            }
            sampleResult.setSuccessful(true);
        } catch (Exception e) {
            sampleResult.setSuccessful(false);
        } finally {
            // 结束
            sampleResult.sampleEnd();
        }

        return sampleResult;
    }
}
