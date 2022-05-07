use crate::common;

pub struct SlotData<T> {
    hash: u64,
    key: Vec<u8>,
    value: T,
}

impl<T> common::SlotData for SlotData<T> {
    type Value = T;

    fn new(key: &[u8], hash: u64, value: Self::Value) -> Self {
        SlotData {
            hash,
            key: key.to_owned(),
            value,
        }
    }

    fn key(&self) -> &[u8] {
        &self.key
    }

    fn hash(&self) -> Option<u64> {
        Some(self.hash)
    }

    fn value(&self) -> &Self::Value {
        &self.value
    }

    fn kv_mut(&mut self) -> (&[u8], &mut Self::Value) {
        (&self.key, &mut self.value)
    }

    fn into_value(self) -> Self::Value {
        self.value
    }

    fn into_kv(self) -> (Vec<u8>, Self::Value) {
        (self.key, self.value)
    }
}

pub type StringMap<T> = common::StringMap<SlotData<T>>;

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use std::hash::BuildHasher;

    use super::*;

    #[test]
    fn test_hash_map() {
        let mut map = StringMap::new();
        let mut cmp = HashMap::new();
        let hasher = RandomState::default();

        for _ in 0..50000 {
            let value = rand::random::<u64>();
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            map.insert(&key, hash, value, &hasher);
            cmp.insert(key.to_vec(), value);
        }

        // Test conflict conditions
        for _ in 0..100000 {
            let value = rand::random::<u16>() as u64;
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            map.insert(&key, hash, value, &hasher);
            cmp.insert(key.to_vec(), value);
        }

        for (k, v) in map {
            let value = cmp.remove(&k);
            assert_eq!(value, Some(v));
        }
        assert!(cmp.is_empty());
    }
}
