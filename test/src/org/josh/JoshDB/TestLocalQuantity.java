package org.josh.JoshDB;

import org.josh.JoshDb.LocalQuantity;
import org.testng.annotations.Test;

import java.util.Random;

public class TestLocalQuantity
{
    @Test
    public void testReceivingConsistency()
    {
        LocalQuantity quantity = new LocalQuantity();

        for (int i = 0; i < 10; i++)
        {
            new Thread(()->
            {
                try
                {
                    //todo use securerandom here
                    Thread.sleep(new Random().nextInt(2000));
                }
                catch (InterruptedException e)
                {
                    System.out.println
                    (
                            "Worker thread was interrupted trying to sleep"
                            + e.getMessage()
                    );
                }
                quantity.receive(10);
            }).start();
        }

        try
        {
            Thread.sleep(5000);
        }
        catch (InterruptedException e)
        {
            System.out.println("Main thread was interrupted trying to sleep");
        }
        assert quantity.currentEstimate() == 100;
    }

    @Test
    public void testGivingConsistency()
    {
        LocalQuantity quantity = new LocalQuantity(100);

        for (int i = 0; i < 10; i++)
        {
            new Thread
            (
                ()->
                    {
                        try
                        {
                            Thread.sleep(new Random().nextInt(2000));
                        }
                        catch (InterruptedException e)
                        {
                            System.out.println
                            (
                                "Worker thread was interrupted trying to sleep"
                                + e.getMessage()
                            );
                        }
                        quantity.giveAway(10);
                    }
            )
            .start();
        }

        try
        {
            Thread.sleep(5000);
        }
        catch (InterruptedException e)
        {
            System.out.println("Main thread was interrupted trying to sleep");
        }
        assert quantity.currentEstimate() == 0;
    }


    @Test
    public void testGivingAndReceivingConsistency()
    {
        LocalQuantity quantity = new LocalQuantity(50);

        for (int i = 0; i < 10; i++)
        {
            final int threadNumber = i;
            new Thread
            (
                ()->
                    {
                        try
                        {
                            Thread.sleep(new Random().nextInt(2000));
                        }
                        catch (InterruptedException e)
                        {
                            System.out.println
                                    (
                                        "Worker thread was interrupted trying to sleep"
                                        + e.getMessage()
                                    );
                        }

                        if (threadNumber % 2 == 0)
                        {
                            quantity.giveAway(10);
                        }
                        else
                        {
                            quantity.receive(10);
                        }
                    }
            )
            .start();
        }

        try
        {
            Thread.sleep(5000);
        }
        catch (InterruptedException e)
        {
            System.out.println("Main thread was interrupted trying to sleep");
        }
        assert quantity.currentEstimate() == 50;
    }


}
