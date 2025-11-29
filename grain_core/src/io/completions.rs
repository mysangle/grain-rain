
use core::fmt::{self, Debug};
use crate::CompletionError;
use std::sync::{Arc, OnceLock};

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
                //CompletionType::Read(r) => r.callback(result),
                CompletionType::Write(w) => w.callback(result),
                //CompletionType::Sync(s) => s.callback(result), // fix
                //CompletionType::Truncate(t) => t.callback(result),
                //CompletionType::Group(g) => g.callback(result),
                //CompletionType::Yield => {}
            };

            result.err()
        });
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
    Write(WriteCompletion),
}

impl Debug for CompletionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            //Self::Read(..) => f.debug_tuple("Read").finish(),
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
