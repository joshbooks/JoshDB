# JoshDB
In this branch I threw out all the features people care about so
I could get the distributed atomic stuff really really right.
 - So first step, get nodes talking to each other with netty.
 - Then get the counter messages figured out.
 - Then get the logic to handle counter messages using Cliff Click's
   counter class really really right.
 - Then do it with Cliff Clicks's HashMap, that'll be fun
 - ????
 - Profit
 - but seriously, then we take the stuff we did here, copy it to a
   different directory, then checkout master, and figure out how to
   put this stuff into master and do the features people care about
   using this stuff
 - So it turns out if inside the messages I store the time at which 
   the messages entered different stages of the message pipeline and
   what message each message is in relation to I should be able to 
   implement something like CoDel across my entire database. This 
   requires the message trie logic from master.
 - This timing information can be deleted as part of the compression/
   archival/consolidation of old messages described in master.
 - let's maybe do some persistence stuff now