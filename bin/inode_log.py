#!/usr/bin/env python
#
# Copyright 2013-2022 Ghent University
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
This script stores the inode usage information for the various mounted GPFS filesystems
in a zip file, named by date and filesystem.

@author Andy Georges (Ghent University)
"""
import gzip
import json
import logging
import os
import time

from vsc.config.base import GENT
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.lustre import LustreOperations
from vsc.utils.script_tools import CLI

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60  # 365 minutes -- little over 6 hours.
INODE_LOG_ZIP_PATH = '/var/log/quota/inode-zips'
INODE_STORE_LOG_CRITICAL = 1

from vsc.filesystem.quota.tools import process_inodes_information, mail_admins


class InlodeLog(CLI):


    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    CLI_OPTIONS = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'location': ('path to store the gzipped files', None, 'store', INODE_LOG_ZIP_PATH),
        'backend': ('Storage backend', None, 'store', 'gpfs'),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
        'mailconfig': ("Full configuration for the mail sender", None, "store", None),
    }


    def do(self, dry_run):
        """
        Get the inode info
        """
        stats = {}

        backend = self.options.backend
        try:
            if backend == 'gpfs':
                storage_backend = GpfsOperations()
            elif backend == 'lustre':
                storage_backend = LustreOperations()
            else:
                logging.error("Backend %s not supported", backend)
                raise

            filesets = storage_backend.list_filesets()
            quota = storage_backend.list_quota()

            if not os.path.exists(self.options.location):
                os.makedirs(self.options.location, 0o755)

            critical_filesets = dict()

            for filesystem in filesets:
                stats["%s_inodes_log_critical" % (filesystem,)] = INODE_STORE_LOG_CRITICAL
                try:
                    filename = "%s_inodes_%s_%s.gz" % (backend, time.strftime("%Y%m%d-%H:%M"), filesystem)
                    path = os.path.join(self.options.location, filename)
                    zipfile = gzip.open(path, 'wb', 9)  # Compress to the max
                    zipfile.write(json.dumps(filesets[filesystem]).encode())
                    zipfile.close()
                    stats["%s_inodes_log" % (filesystem,)] = 0
                    logging.info("Stored inodes information for FS %s", filesystem)

                    cfs = process_inodes_information(filesets[filesystem], quota[filesystem]['FILESET'],
                                                    threshold=0.9, storage=backend)
                    logging.info("Processed inodes information for filesystem %s", filesystem)
                    if cfs:
                        critical_filesets[filesystem] = cfs
                        logging.info("Filesystem %s has at least %d filesets reaching the limit", filesystem, len(cfs))

                except Exception:
                    stats["%s_inodes_log" % (filesystem,)] = 1
                    logging.exception("Failed storing inodes information for FS %s", filesystem)

            logging.info("Critical filesets: %s", critical_filesets)

            if critical_filesets:
                mail_admins(critical_filesets, dry_run=self.options.dry_run, host_institute=self.options.host_institute)


if __name__ == '__main__':
    inode_log = InlodeLog()
    inode_log.main()
