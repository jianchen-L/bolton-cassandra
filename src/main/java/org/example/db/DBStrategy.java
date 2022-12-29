package org.example.db;

import java.time.Instant;

public interface DBStrategy {

    void txnLockMeta(String key, Instant timestamp, long tid);


}
