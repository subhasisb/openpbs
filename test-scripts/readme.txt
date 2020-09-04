How to use:

1. Run build-image.sh <path to pbs src dir>
   NOTE: for multiserver use https://github.com/subhasisb/openpbs/tree/msvr_rapid_proto

2. copy generated <path to pbs src dir>/pbs.tgz to this dir (aka 'test-scripts' - dir in which this readme.txt exists)

3. create file called 'nodes' in this dir and add per line uniq FQDN (IP not supported) of machine which are available for tests
   NOTE1: Make sure you have password less to all nodes
   NOTE2: FQDN should be resolvable
   NOTE3: python3 should be install on first node in list
   NOTE4: Make sure you have podman installed in all nodes
   NOTE5: 'You' must be non-root user on all machines, root user is not supported

4. copy this dir to first node and put it under your home directory

5. run 'python3 master.py &> master.log' from this dir as yourself on first node

6. once above finish, you should have 'results' dir in current dir, copy master.log in it, tar 'results'

7. share results tarball with multiserver team
