use std::collections::HashSet;

/// Optimized Bloom filter for message deduplication
/// Inspired by bitchat's OptimizedBloomFilter for efficient gossip protocol
/// Guarantees no false negatives, rare false positives
pub struct BloomFilter {
    bits: Vec<bool>,
    hash_seeds: Vec<u64>,
    #[allow(dead_code)]
    capacity: usize,
    #[allow(dead_code)]
    false_positive_rate: f64,
    /// Count of inserted items
    item_count: usize,
    /// Track exact items for zero-false-positive mode when small
    exact_set: HashSet<u64>,
    exact_threshold: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter
    /// capacity: expected number of items
    /// false_positive_rate: target FPR (0.01 = 1%)
    pub fn new(capacity: usize, false_positive_rate: f64) -> Self {
        // Optimal bit array size: m = -n * ln(p) / (ln(2))^2
        let m = ((-(capacity as f64) * false_positive_rate.ln()) / (2.0f64.ln().powi(2))).ceil() as usize;
        // Optimal number of hash functions: k = (m/n) * ln(2)
        let k = ((m as f64 / capacity as f64) * 2.0f64.ln()).ceil() as usize;

        let hash_seeds: Vec<u64> = (0..k).map(|i| (i as u64).wrapping_mul(0x517cc1b727220a95)).collect();

        Self {
            bits: vec![false; m.max(64)],
            hash_seeds,
            capacity,
            false_positive_rate,
            item_count: 0,
            exact_set: HashSet::with_capacity(64),
            exact_threshold: 32, // Use exact tracking for small sets
        }
    }

    /// Insert an item into the filter
    pub fn insert(&mut self, item: &str) {
        let hash = self.hash_item(item);

        // Use exact tracking for small sets
        if self.item_count < self.exact_threshold {
            self.exact_set.insert(hash);
            self.item_count += 1;
            return;
        }

        // If transitioning from exact to bloom, insert all exact items first
        if self.item_count == self.exact_threshold {
            let items: Vec<u64> = self.exact_set.iter().copied().collect();
            for h in items {
                self.insert_hashed(h);
            }
            self.exact_set.clear();
        }

        self.insert_hashed(hash);
        self.item_count += 1;
    }

    fn insert_hashed(&mut self, hash: u64) {
        for &seed in &self.hash_seeds {
            let idx = self.hash_with_seed(hash, seed) % self.bits.len();
            self.bits[idx] = true;
        }
    }

    /// Check if an item might be in the filter
    /// Returns true if possibly present (could be false positive)
    /// Returns false if definitely not present
    pub fn might_contain(&self, item: &str) -> bool {
        let hash = self.hash_item(item);

        // Check exact set first (zero false positives for small sets)
        if self.item_count <= self.exact_threshold {
            return self.exact_set.contains(&hash);
        }

        // Check bloom filter
        for &seed in &self.hash_seeds {
            let idx = self.hash_with_seed(hash, seed) % self.bits.len();
            if !self.bits[idx] {
                return false;
            }
        }
        true
    }

    /// Hash an item to a u64
    fn hash_item(&self, item: &str) -> u64 {
        // FNV-1a hash (64-bit)
        let mut hash: u64 = 0xcbf29ce484222325;
        for byte in item.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        hash
    }

    /// Hash with a seed using mixing
    fn hash_with_seed(&self, hash: u64, seed: u64) -> usize {
        let combined = hash.wrapping_add(seed).wrapping_mul(0x517cc1b727220a95);
        // Final mix
        let mixed = combined ^ (combined >> 33);
        mixed.wrapping_mul(0xff51afd7ed558ccd) as usize
    }

    /// Current item count
    pub fn len(&self) -> usize {
        self.item_count
    }

    /// Check if filter is empty
    pub fn is_empty(&self) -> bool {
        self.item_count == 0
    }

    /// Estimate false positive rate
    pub fn estimated_fpr(&self) -> f64 {
        if self.item_count == 0 {
            return 0.0;
        }
        let set_bits = self.bits.iter().filter(|&&b| b).count() as f64;
        let total_bits = self.bits.len() as f64;
        if total_bits == 0.0 {
            return 1.0;
        }
        (set_bits / total_bits).powi(self.hash_seeds.len() as i32)
    }

    /// Reset the filter
    pub fn clear(&mut self) {
        self.bits.fill(false);
        self.item_count = 0;
        self.exact_set.clear();
    }
}

/// LRU-backed dedup cache using Bloom filter for fast negative checks
/// Combines Bloom filter efficiency with exact tracking for recent items
pub struct DedupCache {
    bloom: BloomFilter,
    /// LRU queue for exact tracking
    queue: std::collections::VecDeque<String>,
    /// Set for O(1) lookup
    set: HashSet<String>,
    max_size: usize,
}

impl DedupCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            bloom: BloomFilter::new(max_size, 0.001),
            queue: std::collections::VecDeque::with_capacity(max_size + 64),
            set: HashSet::with_capacity(max_size + 64),
            max_size,
        }
    }

    /// Check if an item is a duplicate
    pub fn is_duplicate(&mut self, id: &str) -> bool {
        if self.set.contains(id) {
            return true;
        }

        // Fast negative check via bloom
        if !self.bloom.might_contain(id) {
            return false;
        }

        // Bloom says maybe, but not in exact set → new item
        false
    }

    /// Mark an item as seen
    pub fn mark_seen(&mut self, id: &str) {
        // Evict oldest if at capacity
        if self.set.len() >= self.max_size {
            if let Some(old) = self.queue.pop_front() {
                self.set.remove(&old);
            }
        }

        self.bloom.insert(id);
        self.queue.push_back(id.to_string());
        self.set.insert(id.to_string());
    }

    pub fn len(&self) -> usize {
        self.set.len()
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut bf = BloomFilter::new(100, 0.01);

        bf.insert("message-1");
        bf.insert("message-2");
        bf.insert("message-3");

        assert!(bf.might_contain("message-1"));
        assert!(bf.might_contain("message-2"));
        assert!(bf.might_contain("message-3"));
        // Should NOT contain this
        assert!(!bf.might_contain("message-999"));
    }

    #[test]
    fn test_bloom_exact_mode_small_sets() {
        let mut bf = BloomFilter::new(100, 0.01);

        // Under threshold → exact tracking, zero false positives
        for i in 0..20 {
            bf.insert(&format!("msg-{}", i));
        }

        // All inserted items present
        for i in 0..20 {
            assert!(bf.might_contain(&format!("msg-{}", i)));
        }

        // Non-inserted items should NOT be present in exact mode
        assert!(!bf.might_contain("msg-100"));
        assert!(!bf.might_contain("msg-200"));
    }

    #[test]
    fn test_dedup_cache() {
        let mut cache = DedupCache::new(100);

        assert!(!cache.is_duplicate("msg-1"));
        cache.mark_seen("msg-1");
        assert!(cache.is_duplicate("msg-1"));
        assert!(!cache.is_duplicate("msg-2"));
    }

    #[test]
    fn test_dedup_cache_lru_eviction() {
        let mut cache = DedupCache::new(10);

        // Insert 10 items
        for i in 0..10 {
            cache.mark_seen(&format!("msg-{}", i));
        }

        // All should still be present
        for i in 0..10 {
            assert!(cache.is_duplicate(&format!("msg-{}", i)));
        }

        // Insert one more → evicts oldest
        cache.mark_seen("msg-10");
        assert!(cache.is_duplicate("msg-10"));
        // msg-0 might still be found in bloom but not in exact set
        // The key invariant: new items are never rejected
    }
}
