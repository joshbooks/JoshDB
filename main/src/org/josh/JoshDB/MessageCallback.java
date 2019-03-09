package org.josh.JoshDB;

@FunctionalInterface
public interface MessageCallback {
    void onMessagePersisted(Message msg);
}
