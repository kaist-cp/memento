
LOCK="mutex rwlock"
LIB="pmdk clobber"

rm memcached.csv
for LB in $LIB; do
	for LK in $LOCK; do
		./build_memcached.sh $LK $LB
		./run_memcache.sh $LK $LB
	done
done

