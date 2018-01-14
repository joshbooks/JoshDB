import java.io.IOException;
import java.nio.file.Paths;

public class TestMain {
    public static void main(String[] args) throws IOException {
        MessagePersistor persistor = new MessagePersistor(Paths.get("/home/flatline/Desktop/testLog"));

        persistor.persistMessage(new Message(), msg -> {


            System.out.println(msg);
        });

        persistor.shutDown();
    }
}
