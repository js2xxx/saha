use std::hash::BuildHasher;
use std::iter::FusedIterator;
use std::{fmt, hint, mem};

const LOAD_FACTOR_N: usize = 3;
const LOAD_FACTOR_D: usize = 2;
const MIN_CAPACITY: usize = 8;

pub trait SlotData {
    type Value;

    fn new(key: &[u8], hash: u64, value: Self::Value) -> Self;

    fn key(&self) -> &[u8];

    fn hash(&self) -> Option<u64>;

    fn value(&self) -> &Self::Value;

    fn kv_mut(&mut self) -> (&[u8], &mut Self::Value);

    fn into_value(self) -> Self::Value;

    fn into_kv(self) -> (Vec<u8>, Self::Value);
}

pub enum Slot<D> {
    Empty,
    Deleted,
    Data(D),
}

impl<D> Slot<D> {
    fn insert(&mut self, data: D) -> &mut D {
        *self = Slot::Data(data);
        unsafe { &mut *Self::data_ptr(self) }
    }

    fn remove(&mut self) -> Option<D> {
        match mem::replace(self, Slot::Deleted) {
            Slot::Data(data) => Some(data),
            _ => None,
        }
    }

    unsafe fn data_ptr(ptr: *mut Slot<D>) -> *mut D {
        match &mut *ptr {
            Slot::Data(data) => data,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

pub struct StringMap<D> {
    bucket: Vec<Slot<D>>,
    len: usize,
}

impl<D> StringMap<D> {
    pub fn new() -> Self {
        Self::with_capacity(MIN_CAPACITY)
    }

    pub fn with_capacity(cap: usize) -> Self {
        let mut bucket = Vec::with_capacity(cap.max(MIN_CAPACITY));
        bucket.resize_with(cap, || Slot::<D>::Empty);
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

impl<D> Default for StringMap<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D: SlotData> StringMap<D> {
    pub fn get(&self, key: &[u8], hash: u64) -> Option<&D::Value> {
        self.lookup(key, hash)
            .map(|ptr| unsafe { (*Slot::data_ptr(ptr)).value() })
    }

    pub fn get_mut(&mut self, key: &[u8], hash: u64) -> Option<&mut D::Value> {
        self.lookup(key, hash)
            .map(|ptr| unsafe { (*Slot::data_ptr(ptr)).kv_mut().1 })
    }

    pub fn insert<S: BuildHasher>(
        &mut self,
        key: &[u8],
        hash: u64,
        value: D::Value,
        hasher: &S,
    ) -> Option<D::Value> {
        let slot = self
            .lookup_or_free(key, hash)
            .expect("Failed to lookup slot");
        let old = mem::replace(unsafe { &mut *slot }, {
            Slot::Data(D::new(key, hash, value))
        });
        self.len += 1;

        if self.len * LOAD_FACTOR_N / LOAD_FACTOR_D >= self.bucket.len() {
            self.resize(self.bucket.len() * 2, hasher);
        }

        match old {
            Slot::Data(data) => Some(data.into_value()),
            _ => None,
        }
    }

    pub fn try_insert<S: BuildHasher>(
        &mut self,
        key: &[u8],
        hash: u64,
        value: D::Value,
        hasher: &S,
    ) -> Option<(&mut D::Value, D::Value)> {
        let slot = self
            .lookup_or_free(key, hash)
            .expect("Failed to lookup slot");
        match unsafe { &mut *slot } {
            Slot::Empty | Slot::Deleted => {
                unsafe { &mut *slot }.insert(D::new(key, hash, value));
                self.len += 1;

                if self.len * LOAD_FACTOR_N / LOAD_FACTOR_D >= self.bucket.len() {
                    self.resize(self.bucket.len() * 2, hasher);
                }

                None
            }
            Slot::Data(data) => Some((data.kv_mut().1, value)),
        }
    }

    pub fn remove<S: BuildHasher>(
        &mut self,
        key: &[u8],
        hash: u64,
        hasher: &S,
    ) -> Option<D::Value> {
        let slot = unsafe { &mut *self.lookup(key, hash)? };
        let ret = slot.remove()?;
        self.len -= 1;

        if self.len > MIN_CAPACITY
            && self.len * LOAD_FACTOR_N / LOAD_FACTOR_D <= self.bucket.len() / 2
        {
            self.resize(self.bucket.len() / 2, hasher);
        }

        Some(ret.into_value())
    }
}

impl<D: SlotData> StringMap<D> {
    fn lookup(&self, key: &[u8], hash: u64) -> Option<*mut Slot<D>> {
        let len = self.bucket.len();
        for i in 0..len {
            let slot = &self.bucket[((hash as usize) + i) % len];
            match slot {
                Slot::Empty => return None,
                Slot::Data(data) if data.key() == key => return Some(slot as *const _ as _),
                _ => {}
            }
        }
        None
    }

    fn lookup_or_free(&self, key: &[u8], hash: u64) -> Option<*mut Slot<D>> {
        let len = self.bucket.len();
        for i in 0..len {
            let slot = &self.bucket[((hash as usize) + i) % len];
            match slot {
                Slot::Empty | Slot::Deleted => return Some(slot as *const _ as _),
                Slot::Data(data) if data.key() == key => return Some(slot as *const _ as _),
                _ => {}
            }
        }
        None
    }

    fn resize<S: BuildHasher>(&mut self, new_len: usize, hasher: &S) {
        let mut bucket = Vec::with_capacity(new_len);
        bucket.resize_with(new_len, || Slot::<D>::Empty);
        let bucket = mem::replace(&mut self.bucket, bucket);
        for item in bucket {
            if let Slot::Data(data) = item {
                let slot = self
                    .lookup_or_free(
                        data.key(),
                        data.hash().unwrap_or_else(|| hasher.hash_one(data.key())),
                    )
                    .unwrap();
                unsafe { (*slot).insert(data) };
            }
        }
    }
}

impl<D> StringMap<D> {
    pub fn iter(&self) -> Iter<D> {
        Iter {
            bucket: &self.bucket,
            rem: self.len,
        }
    }

    pub fn iter_mut(&mut self) -> IterMut<D> {
        IterMut {
            bucket: &mut self.bucket,
            rem: self.len,
        }
    }
}

impl<D: SlotData> IntoIterator for StringMap<D> {
    type Item = (Vec<u8>, D::Value);

    type IntoIter = IntoIter<D>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            bucket: self.bucket.into_iter(),
            rem: self.len,
        }
    }
}

impl<'a, D: SlotData> IntoIterator for &'a StringMap<D> {
    type Item = (&'a [u8], &'a D::Value);

    type IntoIter = Iter<'a, D>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, D: SlotData> IntoIterator for &'a mut StringMap<D> {
    type Item = (&'a [u8], &'a mut D::Value);

    type IntoIter = IterMut<'a, D>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

impl<D: fmt::Debug + SlotData<Value: fmt::Debug>> fmt::Debug for StringMap<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

pub struct Iter<'a, D> {
    bucket: &'a [Slot<D>],
    rem: usize,
}

impl<'a, D: SlotData> Iterator for Iter<'a, D> {
    type Item = (&'a [u8], &'a D::Value);

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
                        break Some((data.key(), data.value()));
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

impl<'a, D: fmt::Debug + SlotData<Value: fmt::Debug>> fmt::Debug for Iter<'a, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.clone()).finish()
    }
}

impl<'a, D> Clone for Iter<'a, D> {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket,
            rem: self.rem,
        }
    }
}

impl<'a, D: SlotData> ExactSizeIterator for Iter<'a, D> {}
impl<'a, D: SlotData> FusedIterator for Iter<'a, D> {}

pub struct IterMut<'a, D> {
    bucket: &'a mut [Slot<D>],
    rem: usize,
}

impl<'a, D> IterMut<'a, D> {
    fn iter(&self) -> Iter<D> {
        Iter {
            bucket: self.bucket,
            rem: self.rem,
        }
    }
}

impl<'a, D: SlotData> Iterator for IterMut<'a, D> {
    type Item = (&'a [u8], &'a mut D::Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match unsafe { &mut *(self.bucket as *mut [Slot<D>]) }.split_first_mut() {
                Some((slot, rem)) => {
                    self.bucket = rem;
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some(data.kv_mut());
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

impl<'a, D: fmt::Debug + SlotData<Value: fmt::Debug>> fmt::Debug for IterMut<'a, D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<'a, D: SlotData> ExactSizeIterator for IterMut<'a, D> {}
impl<'a, D: SlotData> FusedIterator for IterMut<'a, D> {}

pub struct IntoIter<D> {
    bucket: std::vec::IntoIter<Slot<D>>,
    rem: usize,
}

impl<D> IntoIter<D> {
    fn iter(&self) -> Iter<D> {
        Iter {
            bucket: self.bucket.as_slice(),
            rem: self.rem,
        }
    }
}

impl<D: SlotData> Iterator for IntoIter<D> {
    type Item = (Vec<u8>, D::Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.rem == 0 {
            return None;
        }
        loop {
            match self.bucket.next() {
                Some(slot) => {
                    if let Slot::Data(data) = slot {
                        self.rem -= 1;
                        break Some(data.into_kv());
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

impl<D: fmt::Debug + SlotData<Value: fmt::Debug>> fmt::Debug for IntoIter<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<D: SlotData> ExactSizeIterator for IntoIter<D> {}
impl<D: SlotData> FusedIterator for IntoIter<D> {}
