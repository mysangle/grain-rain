
mod app;
mod helper;

use rustyline::{Config, Editor, error::ReadlineError};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, LazyLock},
};

/// LazyLock: 전역 혹은 static 값을 게으르게 초기화하고 싶을 때 사용
///   - 전역(static) 객체가 무겁거나 초기 생성 비용이 비쌀 때
///   - 전역 객체가 다른 전역 객체에 의존할 때
/// : Rust의 static 값은 const 평가가 가능한 값만 넣을 수 있기 때문
/// 
/// PathBuf와 Path의 관계
///   - PathBuf: 소유한 경로
///   - Path: 빌려온 경로
///   - PathBuf는 String, Path는 str에 대응되는 개념이라고 보면 됨.
pub static HOME_DIR: LazyLock<PathBuf> = LazyLock::new(|| dirs::home_dir().expect("could not determine home directory"));

pub static HISTORY_FILE: LazyLock<PathBuf> = LazyLock::new(|| HOME_DIR.join(".grain_history"));

fn rustyline_config() -> Config {
    Config::builder()
        .completion_type(rustyline::CompletionType::List)
        .auto_add_history(true)
        .build()
}

/// tracing 초기화는 GrainRain에서 해준다.
/// 로그가 나오게 하려면 RUST_LOG를 로그 타입과 함께  앞에 넣어준다
/// > RUST_LOG=info cargo run
fn main() -> anyhow::Result<()> {
    let (mut app, _guard) = app::GrainRain::new()?;

    // rustyline은 tty에서만 동작한다.
    if std::io::IsTerminal::is_terminal(&std::io::stdin()) {
        let mut rl = Editor::with_config(rustyline_config())?;
        if HISTORY_FILE.exists() {
            rl.load_history(HISTORY_FILE.as_path())?;
        }
        app = app.with_readline(rl);
    } else {
        tracing::debug!("not in tty");
        // 이후의 readline() 내부 구현을 보면 tty가 아닌 경우(rl을 등록하지 않은 경우)에는 직접 stdin을 읽어서 처리하게 되어 있다.
    }
    
    loop {
        // readline에서 사용자 입력을 받아서 consume에서 처리한다.
        match app.readline() {
            Ok(_) => app.consume(false),
            Err(ReadlineError::Interrupted) => {
                // 인터럽트가 두번 이상 발생한 경우에 루프를 빠져나간다.
                if app.interrupt_count.fetch_add(1, Ordering::SeqCst) >= 1 {
                    eprintln!("Interrupted. Exiting...");
                    break;
                }
                println!("Use .quit to exit or press Ctrl-C again to force quit.");
                app.reset_input();
                continue;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                anyhow::bail!(err);
            }
        }
    }

    Ok(())
}
