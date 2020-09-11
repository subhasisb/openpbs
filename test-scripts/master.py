import json
import os
import pathlib
import socket
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, wait

MYDIR = str(pathlib.Path(__file__).resolve().parent)
ME = socket.getfqdn(socket.gethostname())

def run_cmd(host, cmd):
  if host != ME:
    cmd = ['ssh', host] + cmd
  p = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
  return p.returncode == 0

def copy_artifacts(host):
  if host != ME:
    run_cmd(host, ['mkdir', '-p', MYDIR])
    _c = ['scp', '-p']
    _c += [os.path.join(MYDIR, 'pbs.tgz')]
    _c += [os.path.join(MYDIR, 'entrypoint')]
    _c += [os.path.join(MYDIR, 'submit-jobs.sh')]
    _c += [host + ':' + MYDIR]
    subprocess.run(_c)
  _c = ['podman', 'load', '-i']
  _c += [os.path.join(MYDIR, 'pbs.tgz')]
  _c += ['pbs:latest']
  run_cmd(host, _c)

def cleanup_containers(host):
  print('Cleaning previous containers on %s' % host)
  _c = ['podman', 'ps', '-aqf', 'label=pbs=1']
  _s = []
  if host != ME:
    _s = ['ssh', host]
  try:
      p = subprocess.check_output(_s + _c)
  except:
    return
  p = p.splitlines()
  p = [x.strip() for x in p]
  p = [x for x in p if len(x) > 0]
  if len(p) > 0:
    _c = ['podman', 'rm', '-vif'] + p + ['&>/dev/null']
    run_cmd(host, _c)
  _c = ['podman', 'run', '--network', 'host', '-it']
  _c += ['--rm', '-l', 'pbs=1', '-v', '/tmp:/tmp/htmp']
  _c += ['centos:8', 'rm', '-rf', '/tmp/htmp/pbs']
  _c += ['&>/dev/null']
  run_cmd(host, _c)

def cleanup_system(host):
  print('Cleaning system on %s' % host)
  cleanup_containers(host)
  _c = ['podman', 'rmi', '-f', 'pbs:latest']
  run_cmd(host, _c)

def setup_pbs(host, c, svrs, sips, moms, ncpus):
  _e = os.path.join(MYDIR, 'entrypoint')
  _c = ['podman', 'run', '--network', 'host', '-itd']
  _c += ['--rm', '-l', 'pbs=1', '-v', '%s:%s' % (MYDIR, MYDIR)]
  _c += ['-v', '/tmp/pbs:/var/spool/pbs']
  _c += ['--entrypoint', _e, '--name', c[0]]
  _c += ['--add-host=%s' % x for x in sips]
  _c += ['pbs:latest']
  _c += c[1:]
  if c[2] == 'server':
    if len(moms[c[0]]) > 0:
      _c += [','.join(moms[c[0]])]
      _c += [str(ncpus)]
    else:
      _c += ['none', '0']
  elif c[2] == 'mom':
    _c += [c[0]]
  if len(svrs) > 1:
    _c += [','.join(svrs)]
  p = run_cmd(host, _c)
  if p:
    if c[2] == 'server':
      _c = ['podman', 'exec', c[0]]
      _c += [_e, 'waitsvr', ','.join(moms[c[0]])]
      run_cmd(host, _c)
    print('Configured %s' % c[0])
  else:
    print('Failed to configure %s' % c[0])
  return p

def setup_cluster(tconf, hosts, ips, conf):
  _ts = tconf['total_num_svrs']
  _mph = tconf['num_moms_per_host']
  _cpm = tconf['num_cpus_per_mom']

  _hl = len(hosts)
  _confs = dict([(_h, {'ns': 0, 'svrs': [], 'moms': []}) for _h in hosts])
  _hi = 0
  for i in range(1, _ts + 1):
    _confs[hosts[_hi]]['ns'] += 1
    _hi += 1
    if _hi == _hl:
      _hi = 0
  _tolms = _hl * _mph
  _lps = 18000
  _svrs = []
  _sips = []
  _scnt = 1
  for i, _h in enumerate(hosts):
    for _ in range(_confs[_h]['ns']):
      _c = ['pbs-server-%d' % _scnt, 'default', 'server', str(_scnt)]
      _c.append('1' if _scnt == 1 else '0')
      _p = _lps
      _lps += 2
      _c.append(str(_p))
      _c.append(str(_p + 1))
      _svrs.append('pbs-server-%d:%d' % (_scnt, _p))
      _sips.append('pbs-server-%d:%s' % (_scnt, ips[i]))
      _confs[_h]['svrs'].append(_c)
      _scnt += 1
    for _ in range(_mph):
      _c = ['', 'default', 'mom', str(_lps)]
      _lps += 2
      _confs[_h]['moms'].append(_c)
  for _h in hosts:
    _mc = len(_confs[_h]['moms'])
    i = 0
    while i != _mc:
      for __h in hosts:
        if i == _mc:
          break
        if _confs[__h]['ns'] == 0:
          continue
        for s in _confs[__h]['svrs']:
          _confs[_h]['moms'][i].append(s[3])
          _confs[_h]['moms'][i].append(s[5])
          i += 1
          if i == _mc:
            break
  svrs = dict([(_h, []) for _h in hosts])
  moms = dict([(_h, []) for _h in hosts])
  _mcs = dict([(str(i), 0) for i in range(1, _ts + 1)])
  svrmoms = dict([('pbs-server-%d' % i, []) for i in range(1, _ts + 1)])
  for _h in hosts:
    svrs[_h].extend(_confs[_h]['svrs'])
    for _c in _confs[_h]['moms']:
      _s = _c[4]
      _c[0] = 'pbs-mom-%s-%d' % (_s, _mcs[_s])
      _c[4] = 'pbs-server-%s' % _s
      svrmoms[_c[4]].append('%s:%s:%s' % (_c[0], _h, _c[3]))
      moms[_h].append(_c)
      _mcs[_s] += 1

  with ProcessPoolExecutor(max_workers=len(hosts)) as executor:
    _ps = []
    for _h in hosts:
      _ps.append(executor.submit(cleanup_containers, _h))
    wait(_ps)

  for _h in hosts:
    run_cmd(_h, ['mkdir', '-p', '/tmp/pbs'])

  r = [False]
  with ProcessPoolExecutor(max_workers=10) as executor:
    _ps = []
    for _h, _cs in moms.items():
      for _c in _cs:
        _ps.append(executor.submit(setup_pbs, _h, _c, _svrs, _sips, svrmoms, _cpm))
    for _h, _cs in svrs.items():
      for _c in _cs:
        _ps.append(executor.submit(setup_pbs, _h, _c, _svrs, _sips, svrmoms, _cpm))
    r = list(set([_p.result() for _p in wait(_ps)[0]]))
  if len(r) != 1:
    return False
  return r[0]

def run_tests(conf, hosts, ips):
  for _n, _s in conf['setups'].items():
    print('Configuring setup: %s' % _n)
    r = setup_cluster(_s, hosts, ips, conf)
    if not r:
      print('skipping test, see above for reason')
    else:
      subprocess.run([os.path.join(MYDIR, 'run-test.sh'), _n])

def main():
  if os.getuid() == 0:
    print('This script must be run as non-root user!')
    sys.exit(1)
  if len(sys.argv) > 2:
    print('Usage: python3 master.py [clean]')
    sys.exit(1)

  if len(sys.argv) == 2 and sys.argv[1] != 'clean':
    print('Invalid option: %s' % sys.argv[1])
    print('Usage: python3 master.py [clean]')
    sys.exit(1)

  os.chdir(MYDIR)

  with open('config.json') as f:
    conf = json.load(f)

  if not os.path.isfile('nodes'):
    print('Could not find nodes file')
    sys.exit(1)

  if not os.path.isfile('pbs.tgz'):
    print('Could not find pbs.tgz')
    sys.exit(1)

  hosts = []
  with open('nodes') as f:
    hosts = f.read().splitlines()
    hosts = [_h for _h in hosts if len(_h.strip()) > 0]

  if len(hosts) == 0:
    print('No hosts defined in nodes file')
    sys.exit(1)

  hosts = [socket.getfqdn(_h) for _h in hosts]
  ips = [socket.gethostbyname(_h) for _h in hosts]


  with ProcessPoolExecutor(max_workers=len(hosts)) as executor:
    _ps = []
    for _h in hosts:
      _ps.append(executor.submit(cleanup_system, _h))
    wait(_ps)

  if len(sys.argv) == 2:
    sys.exit(0)

  subprocess.run(['rm', '-rf', 'results'])

  with ProcessPoolExecutor() as executor:
    _ps = []
    for _h in hosts:
      _ps.append(executor.submit(copy_artifacts, _h))
    wait(_ps)

  run_tests(conf, hosts, ips)

  with ProcessPoolExecutor(max_workers=len(hosts)) as executor:
    _ps = []
    for _h in hosts:
      _ps.append(executor.submit(cleanup_system, _h))
    wait(_ps)


if __name__ == '__main__':
  main()
