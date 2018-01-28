package org.josh.JoshDb;

class MessageAndCodeBundle
{
    public Message msg;
    public MessageCallback code;

    public MessageAndCodeBundle(Message msg, MessageCallback function) {
        this.msg = msg;
        this.code = function;
    }

    //default constructor
    public MessageAndCodeBundle() {}
}
