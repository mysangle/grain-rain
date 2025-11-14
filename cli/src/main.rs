
mod app;

/// tracing 초기화는 GrainRain에서 해준다.
/// 로그가 나오게 하려면 RUST_LOG를 로그 타입과 함께  앞에 넣어준다
/// > RUST_LOG=info cargo run
fn main() -> anyhow::Result<()> {
    let (_, _guard) = app::GrainRain::new()?;

    tracing::info!("Hello, Grain Rain!");

    Ok(())
}
