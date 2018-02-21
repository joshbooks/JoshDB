package org.josh.JoshDB;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MapReverse<K, V>
{
    public Map<K, V> reverseMap(Map<V, K> nodeIdToAddress)
    {
        // given the performance implications it does kinda make sense that
        // java doesn't make this as easy as scala does

        Map<K, V> esrever = new HashMap<>();
        nodeIdToAddress
            .forEach
            (
                (key, value) ->
                esrever.put(value, key)
            );
        return esrever;
    }
}
