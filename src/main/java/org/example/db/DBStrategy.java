package org.example.db;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import org.example.common.CqlInfo;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface DBStrategy {

    /**
     * 读取数据
     * @param cqlInfos
     * @return 读取结果
     */
    Collection<String> read(List<CqlInfo> cqlInfos);

    /**
     * 非事务写入
     * @param cqlInfo
     */
    void nonTxnWrite(CqlInfo cqlInfo);

    /**
     * 事务写入
     * @param cqlInfos
     * @param tid
     * @return 异步写入返回对象
     */
    CompletionStage<AsyncResultSet> txnWrite(List<CqlInfo> cqlInfos, long tid);
}
