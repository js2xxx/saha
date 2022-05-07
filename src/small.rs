use std::hash::{BuildHasher, Hash};
use std::iter::FusedIterator;
use std::{fmt, hint, mem};

const LOAD_FACTOR_N: usize = 3;
const LOAD_FACTOR_D: usize = 2;
const MIN_CAPACITY: usize = 8;

struct SlotData<T, const N: usize> {
    key: [u8; N],
    len: usize,
    value: T,
}

enum Slot<T, const N: usize> {
    Empty,
    Deleted,
    Data(SlotData<T, N>),
}

impl<T, const N: usize> Slot<T, N> {
    fn insert(&mut self, data: SlotData<T, N>) -> &mut SlotData<T, N> {
        *self = Slot::Data(data);
        unsafe { &mut *Self::data_ptr(self) }
    }

    fn remove(&mut self) -> Option<SlotData<T, N>> {
        match mem::replace(self, Slot::Deleted) {
            Slot::Data(data) => Some(data),
            _ => None,
        }
    }

    unsafe fn data_ptr(ptr: *mut Slot<T, N>) -> *mut SlotData<T, N> {
        match &mut *ptr {
            Slot::Data(data) => data,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

pub struct StringMap<T, const N: usize> {
    bucket: Vec<Slot<T, N>>,
    len: usize,
}

impl<T, const N: usize> StringMap<T, N> {
    pub fn new() -> Self {
        Self::with_capacity(MIN_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let mut bucket = Vec::with_capacity(cap.max(MIN_CAPACITY));
        bucket.resize_with(cap, || Slot::<T, N>::Empty);
        StringMap { bucket, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<T, const N: usize> Default for StringMap<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Hash, const N: usize> StringMap<T, N> {
    pub fn get(&self, key: &[u8], hash: u64) -> Option<&T> {
        self.lookup(key, hash)
            .map(|ptr| unsafe { &(*Slot::data_ptr(ptr)).value })
    }

    pub fn get_mut(&mut self, key: &[u8], hash: u64) -> Option<&mut T> {
        self.lookup(key, hash)
            .map(|ptr| unsafe { &mut (*Slot::data_ptr(ptr)).value })
    }

    pub fn insert<S: BuildHasher>(
        &mut self,
        key: &[u8],
        hash: u64,
        value: T,
        hasher: &S,
    ) -> Option<T> {
        let slot = self
            .lookup_or_free(key, hash)
            .expect("Failed to lookup slot");
        let old = mem::replace(unsafe { &mut *slot }, {
            let mut data = SlotData {
                key: [0; N],
                len: key.len(),
                value,
            };
            data.key[..data.len].copy_from_slice(key);
            Slot::Data(data)
        });
        self.len += 1;

        if self.len * LOAD_FACTOR_N / LOAD_FACTOR_D >= self.bucket.len() {
            self.resize(self.bucket.len() * 2, hasher);
        }

        match old {
            Slot::Data(data) => Some(data.value),
            _ => None,
        }
    }

    pub fn try_insert<S: BuildHasher>(
        &mut self,
        key: &[u8],
        hash: u64,
        value: T,
        hasher: &S,
    ) -> Option<(&mut T, T)> {
        let slot = self
            .lookup_or_free(key, hash)
            .expect("Failed to lookup slot");
        match unsafe { &mut *slot } {
            Slot::Empty | Slot::Deleted => {
                let mut data = SlotData {
                    key: [0; N],
                    len: key.len(),
                    value,
                };
                data.key[..data.len].copy_from_slice(key);
                unsafe { &mut *slot }.insert(data);
                self.len += 1;

                if self.len * LOAD_FACTOR_N / LOAD_FACTOR_D >= self.bucket.len() {
                    self.resize(self.bucket.len() * 2, hasher);
                }

                None
            }
            Slot::Data(data) => Some((&mut data.value, value)),
        }
    }

    pub fn remove<S: BuildHasher>(&mut self, key: &[u8], hash: u64, hasher: &S) -> Option<T> {
        let slot = unsafe { &mut *self.lookup(key, hash)? };
        let ret = slot.remove()?;
        self.len -= 1;

        if self.len > MIN_CAPACITY
            && self.len * LOAD_FACTOR_N / LOAD_FACTOR_D <= self.bucket.len() / 2
        {
            self.resize(self.bucket.len() / 2, hasher);
        }

        Some(ret.value)
    }
}

impl<T: Hash, const N: usize> StringMap<T, N> {
    fn lookup(&self, key: &[u8], hash: u64) -> Option<*mut Slot<T, N>> {
        let len = self.bucket.len();
        for i in 0..len {
            let slot = &self.bucket[((hash as usize) + i) % len];
            match slot {
                Slot::Empty => return None,
                Slot::Data(data) if &data.key[..data.len] == key => {
                    return Some(slot as *const _ as _)
                }
                _ => {}
            }
        }
        None
    }

    fn lookup_or_free(&self, key: &[u8], hash: u64) -> Option<*mut Slot<T, N>> {
        let len = self.bucket.len();
        for i in 0..len {
            let slot = &self.bucket[((hash as usize) + i) % len];
            match slot {
                Slot::Empty | Slot::Deleted => return Some(slot as *const _ as _),
                Slot::Data(data) if &data.key[..data.len] == key => {
                    return Some(slot as *const _ as _)
                }
                _ => {}
            }
        }
        None
    }

    fn resize<S: BuildHasher>(&mut self, new_len: usize, hasher: &S) {
        let mut bucket = Vec::with_capacity(new_len);
        bucket.resize_with(new_len, || Slot::<T, N>::Empty);
        let bucket = mem::replace(&mut self.bucket, bucket);
        for item in bucket {
            if let Slot::Data(data) = item {
                let slot = self
                    .lookup_or_free(&data.key, hasher.hash_one(&data.key[..data.len]))
                    .unwrap();
                unsafe { (*slot).insert(data) };
            }
        }
    }
}

impl<T, const N: usize> StringMap<T, N> {
    pub fn iter(&self) -> Iter<T, N> {
        Iter {
            bucket: &self.bucket,
            rem: self.len,
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<T, N> {
        IterMut {
            bucket: &mut self.bucket,
            rem: self.len,
        }
    }
}

impl<T, const N: usize> IntoIterator for StringMap<T, N> {
    type Item = (Vec<u8>, T);

    type IntoIter = IntoIter<T, N>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            bucket: self.bucket.into_iter(),
            rem: self.len,
        }
    }
}

impl<'a, T, const N: usize> IntoIterator for &'a StringMap<T, N> {
    type Item = (&'a [u8], &'a T);

    type IntoIter = Iter<'a, T, N>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T, const N: usize> IntoIterator for &'a mut StringMap<T, N> {
    type Item = (&'a [u8], &'a mut T);

    type IntoIter = IterMut<'a, T, N>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<T: fmt::Debug, const N: usize> fmt::Debug for StringMap<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

pub struct Iter<'a, T, const N: usize> {
    bucket: &'a [Slot<T, N>],
    rem: usize,
}

impl<'a, T, const N: usize> Iterator for Iter<'a, T, N> {
    type Item = (&'a [u8], &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match self.bucket.split_first() {
                Some((slot, rem)) => {
                    self.bucket = rem;
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some((&data.key[..data.len], &data.value));
                    }
                }
                None => break None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem, Some(self.rem))
    }
}

impl<'a, T: fmt::Debug, const N: usize> fmt::Debug for Iter<'a, T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.clone()).finish()
    }
}

impl<'a, T, const N: usize> Clone for Iter<'a, T, N> {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket,
            rem: self.rem,
        }
    }
}

impl<'a, T, const N: usize> ExactSizeIterator for Iter<'a, T, N> {}
impl<'a, T, const N: usize> FusedIterator for Iter<'a, T, N> {}

pub struct IterMut<'a, T, const N: usize> {
    bucket: &'a mut [Slot<T, N>],
    rem: usize,
}

impl<'a, T, const N: usize> IterMut<'a, T, N> {
    fn iter(&self) -> Iter<T, N> {
        Iter {
            bucket: self.bucket,
            rem: self.rem,
        }
    }
}

impl<'a, T, const N: usize> Iterator for IterMut<'a, T, N> {
    type Item = (&'a [u8], &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match unsafe { &mut *(self.bucket as *mut [Slot<T, N>]) }.split_first_mut() {
                Some((slot, rem)) => {
                    self.bucket = rem;
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some((&data.key[..data.len], &mut data.value));
                    }
                }
                None => break None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem, Some(self.rem))
    }
}

impl<'a, T: fmt::Debug, const N: usize> fmt::Debug for IterMut<'a, T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'a, T, const N: usize> ExactSizeIterator for IterMut<'a, T, N> {}
impl<'a, T, const N: usize> FusedIterator for IterMut<'a, T, N> {}

pub struct IntoIter<T, const N: usize> {
    bucket: std::vec::IntoIter<Slot<T, N>>,
    rem: usize,
}

impl<T, const N: usize> IntoIter<T, N> {
    fn iter(&self) -> Iter<T, N> {
        Iter {
            bucket: self.bucket.as_slice(),
            rem: self.rem,
        }
    }
}

impl<T, const N: usize> Iterator for IntoIter<T, N> {
    type Item = (Vec<u8>, T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match self.bucket.next() {
                Some(slot) => {
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some((Vec::from(&data.key[..data.len]), data.value));
                    }
                }
                None => break None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.rem, Some(self.rem))
    }
}

impl<T: fmt::Debug, const N: usize> fmt::Debug for IntoIter<T, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<T, const N: usize> ExactSizeIterator for IntoIter<T, N> {}
impl<T, const N: usize> FusedIterator for IntoIter<T, N> {}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::RandomState;
    use std::collections::HashMap;
    use std::hash::BuildHasher;

    use super::*;

    #[test]
    fn test_hash_map() {
        let mut map = StringMap::<u64, 16>::new();
        let mut cmp = HashMap::new();
        let hasher = RandomState::default();

        for _ in 0..10 {
            let value = rand::random::<u64>();
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            let a1 = map.insert(&key, hash, value, &hasher);
            let a2 = cmp.insert(key.to_vec(), value);
            assert_eq!(a1, a2);
        }

        // Test conflict conditions
        for _ in 0..1000 {
            let value = rand::random::<u16>() as u64;
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(key);

            let a1 = map.insert(&key, hash, value, &hasher);
            let a2 = cmp.insert(key.to_vec(), value);
            assert_eq!(a1, a2);
        }

        for (k, &v) in map.iter() {
            let value = cmp.remove(k);
            assert_eq!(value, Some(v));
        }
        assert!(cmp.is_empty());
    }

    #[test]
    fn test_conflict() {
        let mut map = StringMap::<u64, 16>::new();
        let hasher = RandomState::default();

        let value = 111111111111111u64;
        let key = value.to_ne_bytes();
        let hash = hasher.hash_one(&key as &[u8]);

        let a = map.insert(&key, hash, value, &hasher);
        assert_eq!(a, None);

        for _ in 0..7 {
            let value = rand::random::<u64>();
            let key = value.to_ne_bytes();
            let hash = hasher.hash_one(&key as &[u8]);

            map.insert(&key, hash, value, &hasher);
        }
        let a = map.insert(&key, hash, value, &hasher);
        assert_eq!(a, Some(value));
    }
}
