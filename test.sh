
# TODO: 이 파일은 프로젝트에 필요가 없음

set -e

while true; do
    rm -rf test
    RUST_MIN_STACK=10073741824 cargo test --release --features no_persist -- --nocapture
done
