//import sun.reflect.generics.reflectiveObjects.NotImplementedException;
//
//import java.util.concurrent.atomic.AtomicReference;
//import java.util.function.Function;
//
////ok so ima say fuck this, the empty queue/nearly empty queue basically
////would require us to use a different data structure depending on the
////size of the queue, and while that would be crazy cool, and reminds me
////of the synthesis kernel a lot, it is not even vaguely an important component
////of this project, so I think it makes more sense to just use the Disruptor
//// (which is also pretty cool) and do this another time (though I do think the
//// reflection capabilities of java and the new first class status of functions in java 8
////make it fairly  well suited for such an endeavor
//public class MessageQueue {
//
//    //so we'll maintain a head and tail pointer and then both sides should be more or less like
//    //the atomic stack I make interviewees do, maintaining the tail pointer is going to be a
//    //little interesting, but I've already passed the interview
//
//    //ahh crap, this means using a doubly linked list and dealing with replacing both the next and
//    //prev atomically, but no matter, we'll bundle the next and prev together and swap them out
//    //together
//
//    //so ima amend that earlier enthusiasm, because you have to alter two nodes to insert one
//    //you'll need to make sure that the momentary inconsistency isn't going to violate
//    //any guarantees we want to make
//
//    //ooh, because it's a unidirectional queue, there is only one node whose references we need to
//    // change atomically in any given transaction, either the head or the tail need to change the
//    //next or prev respectively
//
//    //except when it's empty/nearly empty, but we'll worry about that when the time is right
//
//    private static class Node
//    {
//        MessageAndCodeBundle payload;
//        AtomicReference<ReferenceBundle> refs;
//
//        public Node(MessageAndCodeBundle bundle)
//        {
//            this(bundle, null, null);
//        }
//
//        public Node(MessageAndCodeBundle bundle, Node next, Node prev)
//        {
//            this.payload = bundle;
//            this.refs = new AtomicReference<>(new ReferenceBundle(next, prev));
//        }
//    }
//
//    private static class ReferenceBundle
//    {
//        Node next;
//        Node prev;
//
//        public ReferenceBundle(Node next, Node prev) {
//            this.next = next;
//            this.prev = prev;
//        }
//    }
//
//
//    private final Node head;
//    private final Node tail;
//
//    public MessageQueue()
//    {
//        this.head = new Node(null);
//        this.tail = new Node(null);
//
//        this.tail.refs.get().prev = head;
//        this.head.refs.get().next = tail;
//    }
//
//    public void enqueue(Message msg, Function function)
//    {
//        enqueue(new MessageAndCodeBundle(msg, function));
//    }
//
//    public void enqueue(MessageAndCodeBundle bundle)
//    {
//        Node newNode = new Node(bundle);
//
//        ReferenceBundle localHeadRefs = head.refs.get();
//        Node next = localHeadRefs.next;
//        ReferenceBundle localNextRefs = next.refs.get();
//
//        newNode.refs.get().next = next;
//        newNode.refs.get().prev = head;
//
//        ReferenceBundle newHeadRefBundle = new ReferenceBundle(newNode, null);
//        ReferenceBundle newNextRefBundle = new ReferenceBundle(localNextRefs.next, newNode);
//
//        //so then these two are going to be sort of interesting
//        next.refs.compareAndSet(localNextRefs, newNextRefBundle);
//        head.refs.compareAndSet(localHeadRefs, newHeadRefBundle);
//
//
//
//    }
//
//    public MessageAndCodeBundle dequeue()
//    {
//
//
//        //make sure we set the refbundle of the node we're dequeueing to null in the right place
//        throw new NotImplementedException();
//    }
//}
//
//
//
//
//
//
