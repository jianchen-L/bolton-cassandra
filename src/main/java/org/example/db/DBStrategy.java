package org.example.db;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.example.common.CqlInfo;

import java.util.Collection;
import java.util.List;

public interface DBStrategy {

    /**
     * 非事务读写
     * @param cql 非事务cql
     * @return 执行结果
     */
    ResultSet nonTxn(String cql);

    /**
     * 读事务
     * @param cqlInfos cql解析结果列表
     * @return 读取结果
     */
    Collection<Row> txnRead(List<CqlInfo> cqlInfos);

    /**
     * 写事务
     * @param cqlInfos cql解析结果列表
     */
    void txnWrite(List<CqlInfo> cqlInfos);
}
