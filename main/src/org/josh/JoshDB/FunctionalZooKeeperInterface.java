package org.josh.JoshDB;

import java.util.Map;

public interface FunctionalZooKeeperInterface
{
    Object withArguments
    (
        Map<String, String> stringArguments,
        Map<String, Long> longArguments
    )
    throws Throwable;
}
