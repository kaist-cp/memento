
# TODO: 이 파일은 프로젝트에 필요가 없음

set -e

# while true; do
#     rm -rf test
#     RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist --features stress smoke -- --nocapture
#     rm -rf test
#     RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist --features stress insert_search -- --nocapture
#     rm -rf test
#     RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist --features stress insert_update_search -- --nocapture
# done

# all
while true; do
    rm -rf test
    RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist insert_search -- --nocapture
    # RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist --features stress queue_gen -- --nocapture
done
