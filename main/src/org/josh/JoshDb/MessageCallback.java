package org.josh.JoshDb;

@FunctionalInterface
public interface MessageCallback {
    void onMessagePersisted(Message msg);
}
