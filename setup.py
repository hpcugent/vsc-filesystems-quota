#!/usr/bin/env python
# -*- coding: latin-1 -*-
# #
# Copyright 2014-2021 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
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
    'version': '1.4.1',
    'author': [ag],
    'maintainer': [ag],
    'excluded_pkgs_rpm': ['vsc', 'vsc.filesystem', 'vsc.filesystem.quota'],
    'setup_requires': ['vsc-install >= 0.15.3'],
    'install_requires': [
        'vsc-accountpage-clients >= 2.0.0',
        'vsc-base >= 3.0.6',
        'vsc-config >= 3.11.0',
        'vsc-filesystems >= 1.3.0',
        'vsc-utils >= 2.0.0',
    ],
    'extras_require': {
        'oceanstor': ['vsc-filesystem-oceanstor >= 0.6.0'],
    },
    'tests_require': ['mock'],
}

if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
