using PageManager.Exceptions;
using System.Collections.Generic;
using System.Linq;

namespace PageManager
{
    public class FifoEvictionPolicy : IPageEvictionPolicy
    {
        private class DoubleLinkedListNode
        {
            public ulong item;
            public DoubleLinkedListNode prev;
            public DoubleLinkedListNode next;

            public (DoubleLinkedListNode, DoubleLinkedListNode) MoveToHead(DoubleLinkedListNode head, DoubleLinkedListNode tail)
            {
                if (this.prev == null)
                {
                    // already a head.
                    return (this, tail);
                }

                this.prev.next = this.next;
                DoubleLinkedListNode tailToReturn = tail;

                if (this.next != null)
                {
                    this.next.prev = this.prev;
                }
                else
                {
                    tailToReturn = this.prev;
                }

                this.next = head;
                this.prev = null;

                if (head != null)
                {
                    head.prev = this;
                }

                return (this, tailToReturn);
            }

            public static (DoubleLinkedListNode, DoubleLinkedListNode) AddToHead(ulong val, DoubleLinkedListNode head, DoubleLinkedListNode tail)
            {
                var node = new DoubleLinkedListNode()
                {
                    item = val,
                    next = head,
                    prev = null
                };

                if (head != null)
                {
                    head.prev = node;
                }
                else
                {
                    return (node, node);
                }

                return (node, tail);
            }

            public (DoubleLinkedListNode, DoubleLinkedListNode) Remove(DoubleLinkedListNode head, DoubleLinkedListNode tail)
            {
                DoubleLinkedListNode newHead = head;
                DoubleLinkedListNode newTail = null;

                if (this.prev != null)
                {
                    this.prev.next = this.next;
                }
                else
                {
                    newHead = this.next;
                }

                if (this.next != null)
                {
                    this.next.prev = this.prev;
                }
                else
                {
                    newTail = this.prev;
                }

                return (newHead, newTail);
            }
        }

        private DoubleLinkedListNode lruHead;
        private DoubleLinkedListNode lruTail;

        // private LinkedList<ulong> pages = new LinkedList<ulong>();
        private readonly ulong pageCountLimit;
        private readonly int evictCountOnReachingLimit;
        private object lck = new object();
        ulong count;
        Dictionary<ulong, DoubleLinkedListNode> nodeMap = new Dictionary<ulong, DoubleLinkedListNode>();

        public FifoEvictionPolicy(ulong pageCountLimit, int evictCountOnReachingLimit)
            => (this.pageCountLimit, this.evictCountOnReachingLimit) = (pageCountLimit, evictCountOnReachingLimit);

        public ulong CurrentPageCount() => count;

        public ulong FreePageCount() => this.pageCountLimit - count;

        public ulong InMemoryPageCountLimit() => this.pageCountLimit;

        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId) => this.RecordUsageAndEvict(pageId, Enumerable.Empty<ulong>());

        public IEnumerable<ulong> RecordUsageAndEvict(ulong pageId, IEnumerable<ulong> pagesToAvoid)
        {
            lock (lck)
            {
                // var node = pages.Find(pageId);
                DoubleLinkedListNode node;
                if (!nodeMap.TryGetValue(pageId, out node))
                {
                    (this.lruHead, this.lruTail) = DoubleLinkedListNode.AddToHead(pageId, this.lruHead, this.lruTail);
                    nodeMap.Add(pageId, this.lruHead);
                    this.count++;
                }
                else
                {
                    (this.lruHead, this.lruTail) = node.MoveToHead(this.lruHead, this.lruTail);
                }

                List<ulong> pagesToRemove = new List<ulong>();
                if (this.count > pageCountLimit)
                {
                    if (this.count - (ulong)pagesToAvoid.Count() < (ulong)this.evictCountOnReachingLimit)
                    {
                        throw new OutOfBufferPoolSpaceException();
                    }

                    int i = 0;
                    var evictCandidate = lruTail;
                    while (i < evictCountOnReachingLimit && evictCandidate != null)
                    {
                        if (pagesToAvoid.Contains(evictCandidate.item))
                        {
                            evictCandidate = evictCandidate.prev;
                        }
                        else
                        {
                            pagesToRemove.Add(evictCandidate.item);
                            nodeMap.Remove(evictCandidate.item);


                            var nextEvictCandidate = evictCandidate.prev;
                            (this.lruHead, this.lruTail) = evictCandidate.Remove(this.lruHead, this.lruTail);
                            evictCandidate = nextEvictCandidate;
                            i++;
                            count--;
                        }
                    }
                }

                return pagesToRemove;
            }
        }
    }
}
