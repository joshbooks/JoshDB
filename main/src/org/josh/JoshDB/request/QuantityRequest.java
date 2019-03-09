package org.josh.JoshDB.request;



public class QuantityRequest extends Request
{
    public QuantityRequest()
    {
        super();
        super.type = RequestType.QUANTITY_REQUEST;
    }



    public enum QuantityRequestType
    {
        RETURN_AVAILABLE, //how many you got?
        AVAILABLE_QUANTITY, //here's how many I got
        GIVE_ME, //gimme some of what you got
        HERES_SOME //take some of what I got
    }

    public QuantityRequestType type;
    public long requestAmount; //ignored for RETURN_AVAILABLE `QuantityRequest`s

}
