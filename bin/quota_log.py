#!/usr/bin/env python
#
# Copyright 2013-2021 Ghent University
#
# This file is part of vsc-filesystems-quota,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-filesystems-quota
#
# vsc-filesystems-quota is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-filesystems-quota is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-filesystems-quota. If not, see <http://www.gnu.org/licenses/>.
#
"""
This script stores the quota information for the various mounted filesystems
in a zip file, named by date and filesystem.

@author Andy Georges
@author Kenneth Waegeman
"""
import gzip
import json
import os
import time

from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.lustre import LustreOperations
from vsc.utils import fancylogger
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60  # 365 minutes -- little over 6 hours.
QUOTA_LOG_ZIP_PATH = '/var/log/quota/zips'

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

QUOTA_STORE_LOG_CRITICAL = 1


def main():
    """The main."""

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'location': ('path to store the gzipped files', None, 'store', QUOTA_LOG_ZIP_PATH),
        'backend': ('Storage backend', None, 'store', 'gpfs'),
    }

    opts = ExtendedSimpleOption(options)

    stats = {}

    backend = opts.options.backend
    try:
        if backend == 'gpfs':
            storage_backend = GpfsOperations()
        elif backend == 'lustre':
            storage_backend = LustreOperations()
        else:
            logger.exception("Backend %s not supported", backend)

        quota = storage_backend.list_quota()

        if not os.path.exists(opts.options.location):
            os.makedirs(opts.options.location, 0o755)

        for key in quota:
            stats["%s_quota_log_critical" % (key,)] = QUOTA_STORE_LOG_CRITICAL
            try:
                filename = "%s_quota_%s_%s.gz" % (backend, time.strftime("%Y%m%d-%H:%M"), key)
                path = os.path.join(opts.options.location, filename)
                zipfile = gzip.open(path, 'wb', 9)  # Compress to the max
                zipfile.write(json.dumps(quota[key]).encode())
                zipfile.close()
                stats["%s_quota_log" % (key,)] = 0
                logger.info("Stored quota information for FS %s", key)
            except Exception:
                stats["%s_quota_log" % (key,)] = 1
                logger.exception("Failed storing quota information for FS %s", key)
    except Exception:
        logger.exception("Failure obtaining %s quota", backend)
        opts.critical("Failure to obtain %s quota information" % backend)

    opts.epilogue("Logged %s quota" % backend, stats)

if __name__ == '__main__':
    main()
