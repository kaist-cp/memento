
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

    # DRAM
    rm -rf test
    mkdir test
    # /home/ubuntu/seungmin.jeon/memento-dev/target/debug/deps/memento-d17820363e14a33a ds::queue_pbcomb::test::enq_deq --exact --nocapture

    # cargo test --features no_persist queue_pbcomb -- --nocapture > ./test/test.json
    cargo test --release --features no_persist queue_pbcomb -- --nocapture > ./test/out.json
    # RUST_BACKTRACE=full RUST_MIN_STACK=10073741824 cargo test --release --features no_persist queue_pbcomb -- --nocapture

    # # PMEM
    # rm -rf /mnt/pmem0/test
    # RUST_MIN_STACK=10073741824 cargo test --release queue_pbcomb -- --nocapture
    # # RUST_MIN_STACK=10073741824 cargo test --release queue_pbcomb -- --nocapture
done

# rm -rf /mnt/pmem0/test
# RUST_MIN_STACK=10073741824 cargo test --release soft_list -- --nocapture
# RUST_MIN_STACK=10073741824 cargo test --release soft_hash -- --nocapture

# while true; do
#     rm -rf /mnt/pmem0/test
#     RUST_MIN_STACK=10073741824 cargo test soft_list -- --nocapture
#     # RUST_MIN_STACK=10073741824 cargo test elim -- --nocapture
# done

