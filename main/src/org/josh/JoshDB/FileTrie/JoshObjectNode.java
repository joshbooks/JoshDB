package org.josh.JoshDB.FileTrie;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The idea is for this class to serve as the provider of the sort of Herlihy
 * method of synchronizing updates to a list, basically by making another copy
 * of a portion of the list.
 *
 * This means enacting some restrictions.
 * What are those restrictions?
 * Well here are a couple that I can think of just off the top of my head:
 * The objects represent updates to objects (maps, lists, strings)
 * updates can contain references to other objects, but those updates
 * must be valid forever.
 * i.e. the update can be undone with another update but the original
 * update is still valid until the second update takes effect.
 */
public class JoshObjectNode
{
  /**
   * Whether the child is the left child or not, defaults to false
   */
  AtomicBoolean goLeft = new AtomicBoolean(false);

  AtomicReference<JoshObjectNode> leftChild = new AtomicReference<>(null);
  AtomicReference<JoshObjectNode> rightChild = new AtomicReference<>(null);

  public final Object self;


  public JoshObjectNode(Object self)
  {
    this.self = self;
  }

  public Object notSureWhatMethodNameShouldBe(JoshObjectNode setTo)
  {

    //IDK????
    // HOW DO I MAKE CONSUMERS DO NOT-STUPID ATOMIC THINGS?
    // I THINK THAT'S THE THING I NEED TO FIGURE OUT
    return null;
  }


}
