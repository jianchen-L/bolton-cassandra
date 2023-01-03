package org.example.service;

public interface RAMPService {

    void nonTxn(String nonTxnCqls);

    void txn(String txnCqls);
}
