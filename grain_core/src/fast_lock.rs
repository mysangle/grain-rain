
use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};

pub struct SpinLockGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Drop for SpinLockGuard<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.store(false, Ordering::Release);
    }
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.value.get() }
    }
}

#[derive(Debug)]
pub struct SpinLock<T> {
    locked: AtomicBool,
    value: UnsafeCell<T>,
}

/// parking_lot::SpinLock을 안쓰고 여기에 간단한 SpinLock을 구현한 이유는 뭘까?
///   - 아주 짧은 시간 동안만 유지될 것이 확실한, 극도로 성능에 민감한 특정 경로에서
///     어떠한 추가적인 오버헤드나 예측 불가능한 동작(스케줄러 양보 등)을 원하지 않아서
///     단순한 락을 직접 구현한 듯?
/// 
/// 오직 `swap`과 `spin_loop`만으로 동작하는, 말 그대로 '무식하게' 회전만 하는 가장 단순한 락
impl<T> SpinLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            locked: AtomicBool::new(false),
            value: UnsafeCell::new(value),
        }
    }

    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        while self.locked.swap(true, Ordering::Acquire) {
            std::hint::spin_loop();
        }
        SpinLockGuard { lock: self }
    }

    pub fn into_inner(self) -> UnsafeCell<T> {
        self.value
    }
}
