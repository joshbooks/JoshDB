import java.util.concurrent.CompletableFuture;

public class PersistThenExecute {
    String thingToBePersisted;
    CompletableFuture<Void> onceItHasBeenPersisted;
}
