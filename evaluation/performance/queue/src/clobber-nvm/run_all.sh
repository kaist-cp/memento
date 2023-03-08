
# run data structures with four libs
cd apps
./nvbench.sh ../../traces
# produce results in apps/data.csv

# generate data for Fig.6
mv data.csv ../fig6.csv

# run log stats and log cost test for clobber-nvm
./logstats.sh ../../traces
./logcost.sh ../../traces
# produce results in apps/logcost.csv and apps/logstats.csv

# generate data for Fig.7
mv logstats.csv ../fig7.csv
cat logcost.csv >> ../fig7.csv

# run memcached and stamp with clobber and pmdk
./buildandrun_memcached.sh
./run_stamp.sh
# produce results in apps/memcached.csv and apps/vacation.csv and yada.csv

# run mnemosyne memcached and vacation
cd ..
./run_mnemosyne.sh
# produce results in vacation_mnemosyne.csv and memcached_mnemosyne.csv

# append results of mnemosyne benchmarks
cat vacation_mnemosyne.csv >> apps/vacation.csv
cat memcached_mnemosyne.csv >> apps/memcached.csv

# generate data for Fig.8 - Fig.10
mv apps/memcached.csv fig8.csv
mv apps/vacation.csv fig9.csv
mv apps/yada.csv fig10.csv

