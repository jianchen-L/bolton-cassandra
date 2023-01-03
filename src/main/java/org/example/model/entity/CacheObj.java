package org.example.model.entity;

import java.util.Set;

public class CacheObj {
    private String key;
    private boolean isTxn;
    private Set<String> writeSet;
}
