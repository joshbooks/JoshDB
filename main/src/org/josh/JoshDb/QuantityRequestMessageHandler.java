package org.josh.JoshDb;

import org.josh.JoshDb.request.QuantityRequest;
import org.josh.JoshDb.request.Request;

import static org.josh.JoshDb.request.Request.RequestType.QUANTITY_REQUEST;

public class QuantityRequestMessageHandler
{
    final LocalQuantity quantity;
    // TODO in order to allow for requests to multiple quantities we'll need to
    // create a higher level data structure that contains multiple LocalQuantities
    // and pass that in as the constructor argument instead of a single LocalQuantity
    public QuantityRequestMessageHandler(LocalQuantity quantity)
    {
        this.quantity = quantity;
    }

    public Message handleMessage(Message quantityRequest) throws WrongMessageTypeException, WrongRequesTypeException
    {
        checkType(quantityRequest);

        QuantityRequest request = (QuantityRequest) quantityRequest.data;

        switch(request.type)
        {
            case RETURN_AVAILABLE:
                return handleReturnAvailable();
            case GIVE_ME:
                return handleGiveMe(request.requestAmount);
            case HERES_SOME:
                return handleHeresSome(request.requestAmount);
            case AVAILABLE_QUANTITY:
                // todo need to update a remote quantity
                // todo create a RemoteQuantity class
                //todo how do we indicate that by returning a request
            default:
                return null;
        }
    }

    private Message handleHeresSome(long requestAmount)
    {
        //at some point want to
        quantity.receive(requestAmount);

        return null;
    }
    //here's the rub, we really want to add to our local quantity only
    // once we've established that the response message is safely persisted
    // globally
    //whereas we really want to take away from our local quantity before we
    //globally persist our response
    //todo this seems like an indication that we're using the wrong abstraction
    // and need to put the message handling logic and the disruptor handling
    // together and make a really cool abstraction out of that

    // I think we might want to put the disruptor bit into a MessageHandlingPipeline
    // and then have MessagePersistor be one of the stages in that pipeline. Then
    // each stage would have a pre- action, so it would go:
    // receiveMessage
    // preReportReceipt
    // reportReceipt
    // persistReceipt
    // preReportIntent
    // reportIntent
    // prePersistIntent
    // persistIntent
    // preReturnResponse
    // returnResponse

    private Message handleGiveMe(long requestAmount)
    {
        //at some point want to
        boolean given = quantity.giveAway(requestAmount);
        return null;
    }

    private Message handleReturnAvailable()
    {
        //at some point want to
        quantity.currentEstimate();
        return null;
    }

    private void checkType(Message quantityRequestMessage) throws WrongMessageTypeException, WrongRequesTypeException
    {
        if (quantityRequestMessage.type != MessageType.REQUEST)
        {
            throw new WrongMessageTypeException("This handler only handles REQUEST Messages");
        }
        Request messageRequest = (Request) quantityRequestMessage.data;

        if (messageRequest.type != QUANTITY_REQUEST)
        {
            throw new WrongRequesTypeException("This handler only handles QUANTITY_REQUEST Requests");

        }
    }
}
