### Checking PMEM status
```bash
sudo ipmctl show -dimm
sudo ipmctl show -topology
sudo ipmctl show -memoryresources
```

### 1. Setting "App-direct" Mode
```bash
# 설치된 PMem 전부 AppDirect 모드로 만들기
sudo ipmctl create -goal MemoryMode=0 PersistentMemoryType=AppDirect
```
AppDirect 모드로 변경했으면 reboot 필요

### 2. Creating namespace
```bash
# 미사용중인 PM을 알아서 가져가서 namespace로 묶음
sudo ndctl create-namespace --mode fsdax
# 이런식으로 이름 설정가능
sudo ndctl create-namespace --mode fsdax -n "MyApp"
```
만들면 아래처럼 출력될 것임 ("fsdax"?: https://docs.pmem.io/ndctl-user-guide/managing-namespaces)
```bash
{
  "dev":"namespace1.0",
  "mode":"fsdax",
  "map":"dev",
  "size":"53.15 GiB (57.07 GB)",
  "uuid":"3879f23c-c3c3-4835-8950-fca3169056fd",
  "sector_size":512,
  "align":2097152,
  "blockdev":"pmem0" # block device "/dev/pmem0"으로 만들어졌다는 뜻
}
```

namespace 만들어진 것 확인
```bash
sudo ndctl list
sudo fdisk -l
ls -l /dev/pmem* # block device 직접 확인
```

### 3. Mount
```bash
sudo mkfs.ext4 /dev/pmem0 # 파일시스템부터 설정해주고 (안해줬었다면)
sudo mount -t ext4 -o dax /dev/pmem0 /mnt/pmem0/ # pm-located 폴더로 mount
df -h # mount list 확인
```
이제 `/mnt/pmem0/`의 파일은 pm-located 파일   (참고: 부팅시마다 자동 mnt되게끔 `/etc/fstab`에  등록해놓는게 좋음. 커맨드로 mnt하면 재부팅마다 unmnt 됨)

## Reference
- https://software.intel.com/content/www/us/en/develop/articles/qsg-part2-linux-provisioning-with-optane-pmem.html#inpage-nav-4