import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class LogFile{

    private final Path logFileLocus;


    public LogFile(Path logFileLocus, int numThreads)
    {
        this.logFileLocus = logFileLocus;
        for (int i = 0; i < numThreads; i++)
        {
            new Thread(() ->
            {
                while (!ControlPanel.shouldQuit)
                {

                }
            }).start();
        }
    }

    //returns a completablefuture that will be triggered when the message has been written
    CompletableFuture<Void> logMessage(Message msg)
    {
        CompletableFuture<Void> whenDone = new CompletableFuture<>();

        logMessage(msg, whenDone);

        return whenDone;
    }

    //the provided completablefuture will be triggered when the message has been written
    void logMessage(Message msg, CompletableFuture future)
    {

    }

    //calls function on message has been persisted
    void logMessage(Message msg, MessageCallback function)
    {

    }
}
