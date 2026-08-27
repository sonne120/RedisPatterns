using System.Diagnostics.CodeAnalysis;

namespace RedisPatterns.Core.Eviction;

public sealed class LruCache<TKey, TValue> where TKey : notnull
{
    private sealed class Node
    {
        public required TKey Key;
        public required TValue Value;
        public Node? Prev;
        public Node? Next;
    }

    private readonly int _capacity;
    private readonly Dictionary<TKey, Node> _map;
    private readonly object _gate = new();
    private Node? _head;
    private Node? _tail;

    public long Hits { get; private set; }
    public long Misses { get; private set; }
    public long Evictions { get; private set; }

    public LruCache(int capacity)
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
            MoveToHead(node);
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
                MoveToHead(existing);
                return;
            }

            if (_map.Count >= _capacity)
            {
                Node victim = _tail!;
                Unlink(victim);
                _map.Remove(victim.Key);
                Evictions++;
            }

            var node = new Node { Key = key, Value = value };
            _map[key] = node;
            LinkAtHead(node);
        }
    }

    public IReadOnlyList<TKey> KeysByRecency()
    {
        lock (_gate)
        {
            var keys = new List<TKey>(_map.Count);
            for (Node? n = _head; n is not null; n = n.Next)
                keys.Add(n.Key);
            return keys;
        }
    }

    private void MoveToHead(Node node)
    {
        if (ReferenceEquals(node, _head)) return;
        Unlink(node);
        LinkAtHead(node);
    }

    private void LinkAtHead(Node node)
    {
        node.Prev = null;
        node.Next = _head;
        if (_head is not null) _head.Prev = node;
        _head = node;
        _tail ??= node;
    }

    private void Unlink(Node node)
    {
        if (node.Prev is not null) node.Prev.Next = node.Next; else _head = node.Next;
        if (node.Next is not null) node.Next.Prev = node.Prev; else _tail = node.Prev;
        node.Prev = node.Next = null;
    }
}
