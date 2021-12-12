
# TODO: 이 파일은 프로젝트에 필요가 없음

set -e

# while true; do
    rm -rf test
    # RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --features no_persist elim -- --nocapture
    RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist insert_search -- --nocapture
    # RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist queue:: -- --nocapture
    # RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist queue:: -- --nocapture
    # RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist treiber_stack -- --nocapture
# done
