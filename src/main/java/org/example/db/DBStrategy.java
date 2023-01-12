package org.example.db;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import org.example.common.CqlInfo;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

public interface DBStrategy {

    /**
     * 读取数据
     * @param cqlInfos cql解析结果列表
     * @return 读取结果
     */
    Collection<String> read(List<CqlInfo> cqlInfos);

    /**
     * 非事务写入
     * @param cqlInfo cql解析结果
     */
    void nonTxnWrite(CqlInfo cqlInfo);

    /**
     * 事务写入
     * @param cqlInfos cql解析结果列表
     * @param tid 事务id
     * @return 异步写入返回对象
     */
    CompletionStage<AsyncResultSet> txnWrite(List<CqlInfo> cqlInfos, long tid);
}
