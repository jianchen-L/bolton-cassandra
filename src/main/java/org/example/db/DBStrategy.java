package org.example.db;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.example.common.CqlInfo;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface DBStrategy {

    /**
     * 非事务读写
     * @param cql 非事务cql
     * @return 执行结果
     */
    ResultSet nonTxn(String cql);

    /**
     * 事务读取
     * @param cqlInfos cql解析结果列表
     * @return 读取结果
     */
    Collection<String> txnRead(List<CqlInfo> cqlInfos);

    /**
     * 事务写入
     * @param cqlInfos cql解析结果列表
     * @param tid 事务id
     * @param timestamp 事务时间戳
     * @return 异步写入返回对象
     */
    CompletionStage<AsyncResultSet> txnWrite(List<CqlInfo> cqlInfos, long tid, Instant timestamp);
}
