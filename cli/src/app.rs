
use crate::{
    helper::GrainHelper,
    HISTORY_FILE,
};
use std::{
    io::{BufRead, IsTerminal},
    mem::ManuallyDrop,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use rustyline::{error::ReadlineError, history::DefaultHistory, Editor};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const PROMPT: &str = "grain> ";

/// input_buff에 ManuallyDrop을 사용하고 있으므로 drop시 해제 필요
pub struct GrainRain {
    pub prompt: String,
    pub rl: Option<Editor<GrainHelper, DefaultHistory>>,
    pub interrupt_count: Arc<AtomicUsize>,
    input_buff: ManuallyDrop<String>,
}

impl GrainRain {
    pub fn new() -> anyhow::Result<(Self, WorkerGuard)> {
        let guard = Self::init_tracing()?;

        let interrupt_count = Arc::new(AtomicUsize::new(0));

        let app = Self {
            prompt: PROMPT.to_string(),
            rl: None,
            interrupt_count,
            input_buff: ManuallyDrop::new("".to_string()),
        };
        Ok((app, guard))
    }

    /// 사용자 입력을 위한 rustyline 등록
    pub fn with_readline(mut self, rl: Editor<GrainHelper, DefaultHistory>) -> Self {
        self.rl = Some(rl);
        self
    }

    /// tracing 초기화
    pub fn init_tracing() -> Result<WorkerGuard, std::io::Error> {
        let ((non_blocking, guard), should_emit_ansi) = {
            (
                tracing_appender::non_blocking(std::io::stderr()),
                IsTerminal::is_terminal(&std::io::stderr()),
            )
        };

        // rustyline=off: rustyline이 찍는 tracing 로그를 제거
        if let Err(e) = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_line_number(true)
                    .with_thread_ids(true)
                    .with_ansi(should_emit_ansi),
            )
            .with(EnvFilter::from_default_env().add_directive("rustyline=off".parse().unwrap()))
            .try_init()
        {
            println!("Unable to setup tracing appender: {e:?}");
        }
        Ok(guard)
    }

    /// 사용자 입력 받기
    pub fn readline(&mut self) -> Result<(), ReadlineError> {
        // write_str을 위해 필요
        use std::fmt::Write;

        if let Some(rl) = &mut self.rl {
            // prompt 출력하고 사용자 입력 받기
            let result = rl.readline(&self.prompt)?;
            let _ = self.input_buff.write_str(result.as_str());
            
        } else {
            // rustyline이 사용되지 않는 경우는 stdio 사용
            let mut reader = std::io::stdin().lock();
            if reader.read_line(&mut self.input_buff)? == 0 {
                return Err(ReadlineError::Eof);
            }
        }

        // multiline으로 동작할 경우에 대한 대비
        let _ = self.input_buff.write_char(' ');
        Ok(())
    }

    /// 사용자 입력 처리
    pub fn consume(&mut self, flush: bool) {
        if self.input_buff.trim().is_empty() {
            return;
        }

        self.reset_line();

        let value = self.input_buff.trim();
        // 사용자 입력 끝에 ';'가 없으면 멀티라인으로 처리한다.
        match (value.starts_with('.'), value.ends_with(';')) {
            (true, _) => {
                if value == ".quit" {
                    self.save_history();
                    // 사용자가 '.quit'를 입력하면 프로세스 종료
                    std::process::exit(0);
                }
                self.reset_input();
            }
            (false, true) => {
                self.reset_input();
            }
            (false, false) if flush => {
                self.reset_input();
            }
            (false, false) => {
                self.set_multiline_prompt();
            }
        }
        
    }

    fn set_multiline_prompt(&mut self) {

    }

    pub fn reset_input(&mut self) {
        self.prompt = PROMPT.to_string();
        self.input_buff.clear();
    }

    fn reset_line(&mut self) {
        self.interrupt_count.store(0, Ordering::Release);
    }

    fn save_history(&mut self) {
        if let Some(rl) = &mut self.rl {
            let _ = rl.save_history(HISTORY_FILE.as_path());
        }
    }
}

impl Drop for GrainRain {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.input_buff);
        }
    }
}

