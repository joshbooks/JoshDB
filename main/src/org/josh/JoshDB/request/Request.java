package org.josh.JoshDB.request;

public abstract class Request
{
    public enum RequestType
    {
        QUANTITY_REQUEST
    }

    public RequestType type;
}
