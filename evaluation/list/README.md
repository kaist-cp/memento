TODO


# Tracking

- time: Each experiment lasts 10 seconds and each data point is the average of 10 experiments
- key: chosen uniformly at random from the range [1, 500]. (Experiments for other ranges
can be found in [2]; they exhibit the same trends as the diagrams here.)
- number of initial items: 250 inserts of random keys, resulting in an almost 40%-full list.
- workload: update-intensive (30% finds) and read-intensive (70% finds) benchmarks.
- Results for other operation type distributions were similar

fig

3a,4a: 처리율 제대로 잰 것 (3: read-intensive, 4: update-intensive)
3b,4a: psync 수
3c,4c: psync를 제외하고 재봄
3e,4e: pwb 수
3f,4f: pwb를 제외하고 재봄

5: tracking에서의 persist cost 분석
6: capsule에서의 persist cost 분석

# 우리

- time: 10초씩 5번
- key: 똑같이 [1, 500]에서 랜덤하게 선택
- number of initial items: 똑같이 250개 랜덤 key
- workload: 똑같이

fig

3a, 4a만 똑같이하면 될듯. 여기서 더 늘리고 싶으면, key range를 달리하면 될듯
