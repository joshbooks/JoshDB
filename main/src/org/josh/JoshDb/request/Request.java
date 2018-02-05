package org.josh.JoshDb.request;

public abstract class Request
{
    public enum RequestType
    {
        QUANTITY_REQUEST
    }

    public RequestType type;
}
