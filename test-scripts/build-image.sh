#!/bin/bash

if [ "x$1" == "x" ]; then
    echo "Usage: $0 <path/to/pbs/source/dir>"
    exit 1
fi

if [ ! -d $1 ]; then
    echo "$1 doesn't exists or not directory"
    exit 1
fi

cd $1

cur_dir=$(pwd)
TMP_DIR=${TMP_DIR:-/tmp}

function build_image() {
    local _ret

    cd ${cur_dir}
    cat >${TMP_DIR}/pp_dockerfile<<__PP_DF__
FROM centos:8 AS base
ENV LANG=C.utf8 LC_ALL=C.utf8
RUN set -ex \\
    && groupadd -g 1900 tstgrp00 \\
    && groupadd -g 1901 tstgrp01 \\
    && groupadd -g 1902 tstgrp02 \\
    && groupadd -g 1903 tstgrp03 \\
    && groupadd -g 1904 tstgrp04 \\
    && groupadd -g 1905 tstgrp05 \\
    && groupadd -g 1906 tstgrp06 \\
    && groupadd -g 1907 tstgrp07 \\
    && groupadd -g 901 pbs \\
    && groupadd -g 1146 agt \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4357 -g tstgrp00 -G tstgrp00 pbsadmin \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 9000 -g tstgrp00 -G tstgrp00 pbsbuild \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 884 -g tstgrp00 -G tstgrp00 pbsdata \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4367 -g tstgrp00 -G tstgrp00 pbsmgr \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4373 -g tstgrp00 -G tstgrp00 pbsnonroot \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4356 -g tstgrp00 -G tstgrp00 pbsoper \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4358 -g tstgrp00 -G tstgrp00 pbsother \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4371 -g tstgrp00 -G tstgrp00 pbsroot \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4355 -g tstgrp00 -G tstgrp02,tstgrp00 pbstest \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4359 -g tstgrp00 -G tstgrp00 pbsuser \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4361 -g tstgrp00 -G tstgrp01,tstgrp02,tstgrp00 pbsuser1 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4362 -g tstgrp00 -G tstgrp01,tstgrp03,tstgrp00 pbsuser2 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4363 -g tstgrp00 -G tstgrp01,tstgrp04,tstgrp00 pbsuser3 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4364 -g tstgrp01 -G tstgrp04,tstgrp05,tstgrp01 pbsuser4 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4365 -g tstgrp02 -G tstgrp04,tstgrp06,tstgrp02 pbsuser5 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4366 -g tstgrp03 -G tstgrp04,tstgrp07,tstgrp03 pbsuser6 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 4368 -g tstgrp01 -G tstgrp01 pbsuser7 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 11000 -g tstgrp00 -G tstgrp00 tstusr00 \\
    && useradd -K UMASK=0022 -m -s /bin/bash -u 11001 -g tstgrp00 -G tstgrp00 tstusr01 \\
    && dnf install -y sudo openssh-server openssh-clients passwd which net-tools bc \\
    && echo 'Defaults  always_set_home' > /etc/sudoers.d/pbs \\
    && echo 'Defaults  !requiretty' >> /etc/sudoers.d/pbs \\
    && echo 'ALL ALL=(ALL)  NOPASSWD: ALL' >> /etc/sudoers.d/pbs \\
    && echo '' > /etc/security/limits.conf \\
	&& rm -f /etc/security/limits.d/*.conf \\
    && ssh-keygen -A \\
    && /usr/sbin/sshd \\
    && ssh-keygen -N "" -C "common-ssh-pair" -f ~/.ssh/id_rsa -t rsa -q \\
    && cp ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys \\
    && echo 'root:pbs' | chpasswd \\
    && for user in \$(awk -F: '/^(pbs|tst)/ {print \$1}' /etc/passwd); do \\
        rm -rf /home/\${user}/.ssh; \\
        cp -rfp ~/.ssh /home/\${user}/; \\
        chown -R \${user}: /home/\${user}/.ssh; \\
        echo "\${user}:pbs" | chpasswd; \\
    done \\
    && echo 'Host *' >> /etc/ssh/ssh_config \\
    && echo '  StrictHostKeyChecking no' >> /etc/ssh/ssh_config \\
    && echo '  ConnectionAttempts 3' >> /etc/ssh/ssh_config \\
    && echo '  IdentityFile ~/.ssh/id_rsa' >> /etc/ssh/ssh_config \\
    && echo '  PreferredAuthentications publickey,password' >> /etc/ssh/ssh_config \\
    && echo 'PermitRootLogin yes' >> /etc/ssh/sshd_config \\
    && dnf -y clean all \\
    && rm -rf /var/run/*.pid /run/nologin /tmp/*

FROM base AS genrpm
COPY . /pbssrc
RUN set -ex \\
    && cd /pbssrc \\
    && ONLY_INSTALL_DEPS=1 /pbssrc/.travis/do.sh \\
    && cd /pbssrc \\
    && git clean -ffdx \\
    && ./autogen.sh \\
    && mkdir target \\
    && cd target \\
    && ../configure --enable-ptl --with-swig=/usr/local \\
    && make dist \\
    && mkdir -p ~/rpmbuild/SOURCES \\
    && cp *.tar.gz ~/rpmbuild/SOURCES/ \\
    && CFLAGS="-g -O2 -Wall -Werror" rpmbuild -ba openpbs.spec --with ptl -D "_with_swig --with-swig=/usr/local" \\
    && mkdir -p /tmp/rpms \\
    && cp -v ~/rpmbuild/RPMS/x86_64/*-server*.rpm /tmp/rpms \\
    && rm -f /tmp/rpms/*-debug*

FROM base
ENV LANG=C.utf8 LC_ALL=C.utf8
COPY --from=genrpm /tmp/rpms /tmp/rpms
RUN set -ex \\
    && dnf install -y /tmp/rpms/* \\
    && rm -rf /var/spool/pbs /etc/pbs.conf* /tmp/* \\
    && dnf -y clean all
__PP_DF__

    _cur_md5=$(md5sum ${TMP_DIR}/pp_dockerfile | awk '{ print $1 }')
    _image_md5=$(podman inspect pbs:latest -f '{{ index .Config.Labels "pbs-md5"}}' 2>/dev/null)
    _src_md5=$(git ls-files | xargs md5sum | md5sum | awk '{ print $1 }')
    _isrc_md5=$(podman inspect pbs:latest -f '{{ index .Config.Labels "pbs-src-md5"}}' 2>/dev/null)
    if [ "x${_sr_cur_md5c_md5}" == "x${_image_md5}" -a "x${_src_md5}" == "x${_isrc_md5}" ]; then
        rm -f ${TMP_DIR}/pp_dockerfile
        return
    fi

    podman build --force-rm --rm -f ${TMP_DIR}/pp_dockerfile -t pbs:latest --label pbs-md5=${_cur_md5} --label pbs-src-md5=${_src_md5} .
    _ret=$?
    rm -f ${TMP_DIR}/pp_dockerfile
    if [ ${_ret} -ne 0 ]; then
        echo "pbs image build failed"
        exit 1
    fi
}

build_image
podman image save pbs:latest | gzip -c > pbs.tgz
