
use crate::{
    helper::GrainHelper,
    HISTORY_FILE,
};
use grain_core::Database;
use rustyline::{error::ReadlineError, history::DefaultHistory, Editor};
use std::{
    io::{BufRead, IsTerminal},
    mem::{forget, ManuallyDrop},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const PROMPT: &str = "grain> ";

struct QueryStatistics {
    io_time_elapsed_samples: Vec<Duration>,
    execute_time_elapsed_samples: Vec<Duration>,
}

/// input_buff에 ManuallyDrop을 사용하고 있으므로 drop시 해제 필요
pub struct GrainRain {
    pub prompt: String,
    pub rl: Option<Editor<GrainHelper, DefaultHistory>>,
    pub interrupt_count: Arc<AtomicUsize>,
    input_buff: ManuallyDrop<String>,

    io: Arc<dyn grain_core::IO>,
    conn: Arc<grain_core::Connection>,
}

impl GrainRain {
    pub fn new() -> anyhow::Result<(Self, WorkerGuard)> {
        let guard = Self::init_tracing()?;

        let db_file = ":memory:";
        let (io, conn) = {
            let (io, db) = Database::open_new(&db_file)?;
            let conn = db.connect()?;
            (io, conn)
        };

        let interrupt_count = Arc::new(AtomicUsize::new(0));

        let app = Self {
            prompt: PROMPT.to_string(),
            rl: None,
            interrupt_count,
            input_buff: ManuallyDrop::new("".to_string()),
            io,
            conn,
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

        /// input_buff 처리를 위해 unsafe를 사용하는 이유
        ///   - 메모리 할당을 피하고자 하는 성능 향상
        ///   - run_query()가 &mut self이기 받기 때문에 run_query()에 self.input_buff를 넘겨줄 수 없다
        ///     : 가변 참조와 불변 참조를 동시에 넘겨줄 수 없다.
        ///   - input_buff로부터 새로운 소유권을 가진 String을 만들어서 이를 run_query()에 사용
        /// 
        /// input_buff가 가진 메모리 버퍼를 논리적으로 두 개로 분리
        ///   - 현재까지 입력된 내용이 담긴 String
        ///   - 뒷 부분의 포인터 주소값
        /// 
        /// ManuallyDrop을 사용하여 컴파일러의 자동 drop을 방지
        /// GrainRain이 drop될 때 input_buff를 수동으로 drop 해준다.
        fn take_usable_part(app: &mut GrainRain) -> (String, usize) {
            let ptr = app.input_buff.as_mut_ptr();
            // 변경하기 전 length와 capacity
            let (len, cap) = (app.input_buff.len(), app.input_buff.capacity());
            // input_buff가 기존 String의 끝지점을 가리키도록 변경
            app.input_buff = ManuallyDrop::new(unsafe { String::from_raw_parts(ptr.add(len), 0, cap - len) });
            (unsafe { String::from_raw_parts(ptr, len, len) },  unsafe {
                ptr.add(len).addr()
            })
        }

        /// part: 두 부분으로 나눈 후 앞부분
        /// old_address: 두 부분으로 나눈 후 뒷부분을 가리키는 포인터 주소값
        /// input_buff가 다시 앞부분을 가리키도록 변경한다.
        fn concat_usable_part(app: &mut GrainRain, mut part: String, old_address: usize) {
            let ptr = app.input_buff.as_mut_ptr();
            let (len, cap) = (app.input_buff.len(), app.input_buff.capacity());

            if ptr.addr() != old_address || !app.input_buff.is_empty() {
                // 만약 뒷 부분이 변경되었다면 part는 drop한다.
                return;
            }

            let head_ptr = part.as_mut_ptr();
            let (head_len, head_cap) = (part.len(), part.capacity());
            // part가 drop되지 않도록 한다.
            forget(part);
            app.input_buff = ManuallyDrop::new(unsafe {
                String::from_raw_parts(head_ptr, head_len + len, head_cap + cap)
            })
        }

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
                let (owned_value, old_address) = take_usable_part(self);
                // 쿼리 실행
                self.run_query(owned_value.trim());
                concat_usable_part(self, owned_value, old_address);
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

    fn run_query(&mut self, input: &str) {
        let start = Instant::now();
        let mut stats = Some(QueryStatistics {
            io_time_elapsed_samples: vec![],
            execute_time_elapsed_samples: vec![],
        });

        let conn = self.conn.clone();
        let runner = conn.query_runner(input.as_bytes());

        self.print_query_performance_stats(start, stats.as_ref());
    }

    fn print_query_performance_stats(&mut self, start: Instant, stats: Option<&QueryStatistics>) {

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

