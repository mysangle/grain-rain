
use core::fmt::{self, Debug};
use crate::{Buffer, CompletionError};
use std::sync::{Arc, OnceLock};

pub type ReadComplete = dyn Fn(Result<(Arc<Buffer>, i32), CompletionError>) + Send + Sync;
pub type WriteComplete = dyn Fn(Result<i32, CompletionError>) + Send + Sync;

#[must_use]
#[derive(Clone, Debug)]
pub struct Completion {
    pub(super) inner: Option<Arc<CompletionInner>>,
}

impl Completion {
    pub fn new(completion_type: CompletionType) -> Self {
        Self {
            inner: Some(Arc::new(CompletionInner::new(completion_type, false))),
        }
    }

    pub fn new_write<F>(complete: F) -> Self
    where
        F: Fn(Result<i32, CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Write(WriteCompletion::new(Box::new(
            complete,
        ))))
    }

    pub fn new_read<F>(buf: Arc<Buffer>, complete: F) -> Self
    where
        F: Fn(Result<(Arc<Buffer>, i32), CompletionError>) + Send + Sync + 'static,
    {
        Self::new(CompletionType::Read(ReadCompletion::new(
            buf,
            Box::new(complete),
        )))
    }

    pub fn new_yield() -> Self {
        Self { inner: None }
    }

    pub(super) fn get_inner(&self) -> &Arc<CompletionInner> {
        self.inner
            .as_ref()
            .expect("completion inner should be initialized")
    }

    pub fn complete(&self, result: i32) {
        let result = Ok(result);
        self.callback(result);
    }

    fn callback(&self, result: Result<i32, CompletionError>) {
        let inner = self.get_inner();
        inner.result.get_or_init(|| {
            match &inner.completion_type {
                CompletionType::Read(r) => r.callback(result),
                CompletionType::Write(w) => w.callback(result),
                //CompletionType::Sync(s) => s.callback(result), // fix
                //CompletionType::Truncate(t) => t.callback(result),
                //CompletionType::Group(g) => g.callback(result),
                //CompletionType::Yield => {}
            };

            result.err()
        });
    }

    /// Checks if the Completion completed or errored
    pub fn finished(&self) -> bool {
        match &self.inner {
            Some(inner) => match &inner.completion_type {
                //CompletionType::Group(g) => g.inner.outstanding.load(Ordering::SeqCst) == 0,
                _ => inner.result.get().is_some(),
            },
            None => true,
        }
    }

    pub fn failed(&self) -> bool {
        match &self.inner {
            Some(inner) => inner.result.get().is_some_and(|val| val.is_some()),
            None => false,
        }
    }

    pub fn error(&self, err: CompletionError) {
        let result = Err(err);
        self.callback(result);
    }

    pub fn as_read(&self) -> &ReadCompletion {
        let inner = self.get_inner();
        match inner.completion_type {
            CompletionType::Read(ref r) => r,
            _ => unreachable!(),
        }
    }
}

pub(super) struct CompletionInner {
    completion_type: CompletionType,
    pub(super) result: std::sync::OnceLock<Option<CompletionError>>,
    needs_link: bool,
}

impl fmt::Debug for CompletionInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompletionInner")
            .field("completion_type", &self.completion_type)
            .field("needs_link", &self.needs_link)
            //.field("parent", &self.parent.get().is_some())
            .finish()
    }
}

impl CompletionInner {
    fn new(completion_type: CompletionType, needs_link: bool) -> Self {
        Self {
            completion_type,
            result: OnceLock::new(),
            needs_link,
        }
    }
}

pub enum CompletionType {
    Read(ReadCompletion),
    Write(WriteCompletion),
}

impl Debug for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(..) => f.debug_tuple("Read").finish(),
            Self::Write(..) => f.debug_tuple("Write").finish(),
            //Self::Sync(..) => f.debug_tuple("Sync").finish(),
            //Self::Truncate(..) => f.debug_tuple("Truncate").finish(),
            //Self::Group(..) => f.debug_tuple("Group").finish(),
            //Self::Yield => f.debug_tuple("Yield").finish(),
        }
    }
}

pub struct WriteCompletion {
    pub complete: Box<WriteComplete>,
}

impl WriteCompletion {
    pub fn new(complete: Box<WriteComplete>) -> Self {
        Self { complete }
    }

    pub fn callback(&self, bytes_written: Result<i32, CompletionError>) {
        (self.complete)(bytes_written);
    }
}

pub struct ReadCompletion {
    pub buf: Arc<Buffer>,
    pub complete: Box<ReadComplete>,
}

impl ReadCompletion {
    pub fn new(buf: Arc<Buffer>, complete: Box<ReadComplete>) -> Self {
        Self { buf, complete }
    }

    pub fn buf(&self) -> &Buffer {
        &self.buf
    }

    pub fn callback(&self, bytes_read: Result<i32, CompletionError>) {
        (self.complete)(bytes_read.map(|b| (self.buf.clone(), b)));
    }

    pub fn buf_arc(&self) -> Arc<Buffer> {
        self.buf.clone()
    }
}
