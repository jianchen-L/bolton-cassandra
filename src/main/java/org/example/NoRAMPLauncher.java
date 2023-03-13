package org.example;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.example.db.DBStrategy;
import org.example.db.impl.CassandraImpl;

import java.util.Random;
import java.util.UUID;

public class NoRAMPLauncher extends AbstractJavaSamplerClient {
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
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        // 创建SampleResult对象，用于记录执行结果的状态，并返回
        SampleResult sampleResult = new SampleResult();
        Random pages = new Random();
        Random year = new Random();
        // 开始
        sampleResult.sampleStart();
        try {
            for (int i = 1; i <= 1000; i++) {
                if (i % 33 != 0) {
                    dbStrategy.nonTxn(String.format("INSERT INTO store_no_ramp.books_by_title (title, author_id, pages, year)VALUES ('Book %d', %s, %d, %d);", i, UUID.randomUUID(), pages.nextInt(100, 999), year.nextInt(1900, 2023)));
                }
                if (i % 3 == 0) {
                    dbStrategy.nonTxn(String.format("SELECT title, author_id FROM store_no_ramp.books_by_title WHERE title = 'Book %d';", i / 2));
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