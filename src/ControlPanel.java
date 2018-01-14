/**
 * A bunch of hacky switches that control everything
 */
public class ControlPanel
{
    public static boolean shouldQuit = false;


    public static void quit()
    {
        shouldQuit = true;
    }
}
