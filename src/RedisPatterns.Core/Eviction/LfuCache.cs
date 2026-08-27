using System.Diagnostics.CodeAnalysis;

namespace RedisPatterns.Core.Eviction;

public sealed class LfuCache<TKey, TValue> where TKey : notnull
{
    private sealed class Node
    {
        public required TKey Key;
        public required TValue Value;
        public long Frequency = 1;
        public LinkedListNode<Node>? ListNode;
    }

    private readonly int _capacity;
    private readonly Dictionary<TKey, Node> _map;
    private readonly Dictionary<long, LinkedList<Node>> _freqBuckets = new();
    private readonly object _gate = new();
    private long _minFreq = 1;

    public long Hits { get; private set; }
    public long Misses { get; private set; }
    public long Evictions { get; private set; }

    public LfuCache(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _capacity = capacity;
        _map = new Dictionary<TKey, Node>(capacity);
    }

    public int Count { get { lock (_gate) return _map.Count; } }

    public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue value)
    {
        lock (_gate)
        {
            if (!_map.TryGetValue(key, out Node? node))
            {
                Misses++;
                value = default;
                return false;
            }
            Hits++;
            Touch(node);
            value = node.Value;
            return true;
        }
    }

    public void Put(TKey key, TValue value)
    {
        lock (_gate)
        {
            if (_map.TryGetValue(key, out Node? existing))
            {
                existing.Value = value;
                Touch(existing);
                return;
            }

            if (_map.Count >= _capacity)
                EvictOne();

            var node = new Node { Key = key, Value = value };
            _map[key] = node;
            AddToBucket(node, 1);
            _minFreq = 1;
        }
    }

    public void DecayAll()
    {
        lock (_gate)
        {
            var nodes = _map.Values.ToArray();
            _freqBuckets.Clear();
            _minFreq = long.MaxValue;
            foreach (Node node in nodes)
            {
                node.Frequency = Math.Max(1, node.Frequency / 2);
                AddToBucket(node, node.Frequency);
                _minFreq = Math.Min(_minFreq, node.Frequency);
            }
            if (_map.Count == 0) _minFreq = 1;
        }
    }

    public IReadOnlyList<(TKey Key, long Frequency)> Snapshot()
    {
        lock (_gate)
            return _map.Values.Select(n => (n.Key, n.Frequency)).OrderByDescending(t => t.Frequency).ToArray();
    }

    private void Touch(Node node)
    {
        long oldFreq = node.Frequency;
        RemoveFromBucket(node);

        if (oldFreq == _minFreq && !_freqBuckets.ContainsKey(oldFreq))
            _minFreq = oldFreq + 1;

        node.Frequency = oldFreq + 1;
        AddToBucket(node, node.Frequency);
    }

    private void EvictOne()
    {
        LinkedList<Node> bucket = _freqBuckets[_minFreq];
        Node victim = bucket.First!.Value;
        RemoveFromBucket(victim);
        _map.Remove(victim.Key);
        Evictions++;
    }

    private void AddToBucket(Node node, long freq)
    {
        if (!_freqBuckets.TryGetValue(freq, out LinkedList<Node>? bucket))
            _freqBuckets[freq] = bucket = new LinkedList<Node>();
        node.ListNode = bucket.AddLast(node);
    }

    private void RemoveFromBucket(Node node)
    {
        LinkedList<Node> bucket = _freqBuckets[node.Frequency];
        bucket.Remove(node.ListNode!);
        if (bucket.Count == 0) _freqBuckets.Remove(node.Frequency);
        node.ListNode = null;
    }
}
