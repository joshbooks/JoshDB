@FunctionalInterface
public interface MessageCallback {
    void onMessagePersisted(Message msg);
}
