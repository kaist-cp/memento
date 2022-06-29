
#!/bin/bash
# TODO: 이 파일은 테스트용. repo 공개할때 삭제해야함

# set -e
SCRIPT_DIR=`dirname $(realpath "$0")`
function dmsg() {
    msg=$1
    time=$(date +%m)/$(date +%d)-$(date +%H):$(date +%M)
    echo -e "$msg"
    echo "[$time] $msg" >> $SCRIPT_DIR/repeat.out
}


i=0
bug_cnt=0
while true; do
    i=$(($i+1))
    dmsg "${i}th repeat: run thread crash_recovery.sh"

    $SCRIPT_DIR/tcrash_recovery.sh

    ext=$?
    if [ $ext -ne 0 ]; then
        dmsg "exit with code $ext!"

        src=$SCRIPT_DIR/out_threadcrash
        dest_dir=$SCRIPT_DIR/out_threadcrash_bugs_$(date +%m%d)
        mkdir -p $dest_dir

        # Save log
        cp -r $src $dest_dir/bug$bug_cnt
        cp -r $SCRIPT_DIR/repeat.out $dest_dir/bug$bug_cnt/repeat.out

        # clear
        rm $SCRIPT_DIR/repeat.out
        pkill -9 memento*

        # Next
        i=0
        bug_cnt=$(($bug_cnt+1))
    fi
done
