#!/usr/bin/env python
# #
# Copyright 2014-2023 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
# #
"""
vsc-filesystems-quota base distribution setup.py

@author: Stijn De Weirdt (Ghent University)
@author: Kenneth Waegeman (Ghent University)
@author: Andy Georges (Ghent University)
"""
import vsc.install.shared_setup as shared_setup
from vsc.install.shared_setup import ag

PACKAGE = {
    'version': '2.2.1',
    'author': [ag],
    'maintainer': [ag],
    'excluded_pkgs_rpm': ['vsc', 'vsc.filesystem', 'vsc.filesystem.quota'],
    'setup_requires': ['vsc-install >= 0.15.3'],
    'install_requires': [
        'diskcache >= 5.4.0',
        'vsc-accountpage-clients >= 2.0.0',
        'vsc-base >= 3.0.6',
        'vsc-config >= 3.16.0',
        'vsc-filesystems >= 2.0.0',
        'vsc-kafka',
    ],
    'extras_require': {
        'oceanstor': ['vsc-filesystem-oceanstor >= 0.6.0'],
    },
    'tests_require': ['mock'],
}

if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
