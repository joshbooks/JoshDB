package org.josh.JoshDB;

import java.util.List;

public class HostPortPair
{
    String host;
    int port;

    public HostPortPair(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public static String toConnectionString(List<HostPortPair> hosts)
    {
        StringBuilder connectionStringBuilder = new StringBuilder();

        for (HostPortPair i : hosts)
        {
            connectionStringBuilder.append(i.host).append(":").append(Integer.toString(i.port)).append(",");
        }

        //remove trailing comma
        if (connectionStringBuilder.length() > 0)
        {
            connectionStringBuilder.substring(0, connectionStringBuilder.length()-1);
        }

        return connectionStringBuilder.toString();
    }
}
