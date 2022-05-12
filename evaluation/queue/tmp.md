
```
let v1, v2;
let seq = c.checkpoint(seq)   // seq: 0
if seq가 첫 값 그대로 {
    seq++;
    v1 = q.deq(seq)      // deq seq: 1
    v1 백업
    seq++;
    v2 = q.deq(seq)      // deq seq: 2
} elif seq = 1 {
    v1 = q.deq(seq);     // deq seq: 1
    v1 백업
    seq++;
    v2 = q.deq(seq);     // deq seq: 2
} elif seq = 2 {
    v1 = 백업한 값
    v2 = q.deq(seq)      // deq seq: 2
}



q.enq(val: v1+v2, seq: 1);  // enq seq: 1

```
