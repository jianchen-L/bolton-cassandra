package org.example;

import org.example.service.impl.RAMPCassandraImpl;
import org.example.service.RAMPService;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    private RAMPService RAMPService;

    public void main(String cql) {
        RAMPService = new RAMPCassandraImpl();
        cql = cql.trim().toLowerCase();
        Pattern txnPattern = Pattern.compile("begin[\\s\\n\\r]+ra[\\s\\n\\r]*;[\\s\\S]*?end[\\s\\n\\r]*;");
        Matcher txnMatcher = txnPattern.matcher(cql);
        int nonTxnStart = 0;
        String nonTxnPart;
        while (txnMatcher.find()) {
            nonTxnPart = cql.substring(nonTxnStart, txnMatcher.start());
            if (!nonTxnPart.isBlank()) {
                RAMPService.nonTxn(nonTxnPart);
            }
            nonTxnStart = txnMatcher.end() + 1;
            String txnPart = txnMatcher.group().replaceFirst("begin[\\s\\n\\r]+ra[\\s\\n\\r]*;", "").replaceFirst("end[\\s\\n\\r]*;", "");
            RAMPService.txn(txnPart);
        }
        if (nonTxnStart < cql.length()) {
            nonTxnPart = cql.substring(nonTxnStart);
            if (!nonTxnPart.isBlank()) {
                RAMPService.nonTxn(nonTxnPart);
            }
        }
    }
}