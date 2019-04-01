package org.josh.JoshDB.FileTrie;

public abstract class PageInfo
{

  public static BasicPageInfo basicInfoFromPage(byte[] page, long offset)
  {
    BasicPageInfo info = new BasicPageInfo();

    info.sequenceNumber = MergeFile.sequenceNumberOfPage(page);
    info.offset = offset;
    info.amountRemaining = MergeFile.amountRemainingForPage(page);

    return info;
  }

  // Cuz I'm a basic bitch
  public static class BasicPageInfo extends PageInfo
  {
    int amountRemaining;
    long sequenceNumber;
    long offset;
  }
}