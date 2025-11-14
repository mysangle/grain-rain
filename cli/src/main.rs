
mod app;
mod helper;

use rustyline::{Config, Editor, error::ReadlineError};
use std::sync::atomic::Ordering;

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
        let rl = Editor::with_config(rustyline_config())?;
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
