static int tas_priority_lock_tm(tas_lock_t *lk) {
  int tries = 0;
  if (spec_entry == 0) { // not in HTM
    TM_STATS_ADD(my_tm_stats->locks, 1);
    tas_lock_t copy;
    int s = spin_begin();
    while(true){
        copy.all = lk->all;
        if(copy.ready==0){
            if (!tatas(&lk->val, 1)) {
                break;
            }
        }
        if(copy.cnt < TK_MAX_DISTANCE-TK_MIN_DISTANCE){
            int tmp = __sync_fetch_and_add(&lk->cnt,1);
			if(tmp < TK_MAX_DISTANCE-TK_MIN_DISTANCE){
				__sync_fetch_and_add(&lk->cnt,-1);
			}
            else if(enter_htm(lk)==0){return 0;}
			else{
				__sync_fetch_and_add(&lk->cnt,-1);
				__sync_fetch_and_add(&lk->ready,1);
				while (tatas(&lk->val, 1)){}
				__sync_fetch_and_add(&lk->ready,-1);
				break;
			}
        }
        s = spin_wait(s);
    }
  }
  TM_STATS_SUB(my_tm_stats->cycles, rdtsc());
  return 0;
}