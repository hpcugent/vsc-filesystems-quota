#
# Copyright 2015-2022 Ghent University
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
Inode logs and notitications.

@author: Andy Georges (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

import gzip
import json
import logging
import os
import socket
import time

from collections import namedtuple

from vsc.config.base import VscStorage, GENT, INSTITUTE_ADMIN_EMAIL
from vsc.filesystem.operator import StorageOperator
from vsc.utils.mail import VscMail
from vsc.utils.script_tools import CLI

NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60  # 365 minutes -- little over 6 hours.
INODE_LOG_ZIP_PATH = '/var/log/quota/inode-zips'
INODE_STORE_LOG_CRITICAL = 1

InodeCritical = namedtuple("InodeCritical", ['used', 'allocated', 'maxinodes'])

CRITICAL_INODE_COUNT_MESSAGE = """
Dear HPC admins,

The following filesets will be running out of inodes soon (or may already have run out).

%(fileset_info)s

Kind regards,
Your friendly inode-watching script
"""


class InodeLog(CLI):

    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    CLI_OPTIONS = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'location': ('path to store the gzipped files', None, 'store', INODE_LOG_ZIP_PATH),
        'storage': ('the VSC filesystems that are checked by this script', None, 'extend', []),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
        'mailconfig': ("Full configuration for the mail sender", None, "store", None),
    }

    def mail_admins(self, critical_filesets, dry_run=True, host_institute=GENT):
        """Send email to the HPC admin about the inodes running out soonish."""
        mail = VscMail(mail_config=self.options.mailconfig)

        message = CRITICAL_INODE_COUNT_MESSAGE
        fileset_info = []
        for (fs_name, fs_info) in critical_filesets.items():
            for (fileset_name, inode_info) in fs_info.items():
                fileset_info.append("%s - %s: used %d (%d%%) of max %d [allocated: %d]" %
                                    (fs_name,
                                    fileset_name,
                                    inode_info.used,
                                    int(inode_info.used * 100 / inode_info.maxinodes),
                                    inode_info.maxinodes,
                                    inode_info.allocated))

        message = message % ({'fileset_info': "\n".join(fileset_info)})

        if dry_run:
            logging.info("Would have sent this message: %s", message)
        else:
            mail.sendTextMail(mail_to=INSTITUTE_ADMIN_EMAIL[host_institute],
                            mail_from=INSTITUTE_ADMIN_EMAIL[host_institute],
                            reply_to=INSTITUTE_ADMIN_EMAIL[host_institute],
                            mail_subject="Inode space(s) running out on %s" % (socket.gethostname()),
                            message=message)


    def do(self, dry_run):
        """
        Get the inode info
        """
        stats = {}

        if not os.path.exists(self.options.location):
            os.makedirs(self.options.location, 0o755)

        storage = VscStorage()

        if len(self.options.storage) > 0:
            target_storage = self.options.storage
        else:
            target_storage = [self.options.host_institute]

        for storage_name in target_storage:
            operator = StorageOperator(storage[storage_name])

            filesets = operator().list_filesets()
            quota = operator().list_quota()

            critical_filesets = dict()

            for filesystem in filesets:
                stats["%s_inodes_log_critical" % (filesystem,)] = INODE_STORE_LOG_CRITICAL
                try:
                    filename = "%s_inodes_%s_%s.gz" % (storage_name, time.strftime("%Y%m%d-%H:%M"), filesystem)
                    path = os.path.join(self.options.location, filename)
                    zipfile = gzip.open(path, 'wb', 9)  # Compress to the max
                    zipfile.write(json.dumps(filesets[filesystem]).encode())
                    zipfile.close()
                    stats["%s_inodes_log" % (filesystem,)] = 0
                    logging.info("Stored inodes information for FS %s", filesystem)

                    cfs = process_inodes_information(filesets[filesystem], quota[filesystem]['FILESET'],
                                                    threshold=0.9, storage=storage_name)
                    logging.info("Processed inodes information for filesystem %s", filesystem)
                    if cfs:
                        critical_filesets[filesystem] = cfs
                        logging.info("Filesystem %s has at least %d filesets reaching the limit", filesystem, len(cfs))

                except Exception:
                    stats["%s_inodes_log" % (filesystem,)] = 1
                    logging.exception("Failed storing inodes information for FS %s", filesystem)

            logging.info("Critical filesets: %s", critical_filesets)

            if critical_filesets:
                self.mail_admins(
                    critical_filesets,
                    dry_run=self.options.dry_run,
                    host_institute=self.options.host_institute
                )


def process_inodes_information(filesets, quota, threshold=0.9, storage='gpfs'):
    """
    Determines which filesets have reached a critical inode limit.

    For this it uses the inode quota information passed in the quota argument and compares this with the maximum number
    of inodes that can be allocated for the given fileset. The default threshold is placed at 90%.

    @returns: dict with (filesetname, InodeCritical) key-value pairs
    """
    critical_filesets = dict()

    for (fs_key, fs_info) in filesets.items():
        allocated = int(fs_info['allocInodes']) if storage == 'gpfs' else 0
        maxinodes = int(fs_info['maxInodes']) if storage == 'gpfs' else int(quota[fs_key][0].filesLimit)
        used = int(quota[fs_key][0].filesUsage)

        if maxinodes > 0 and used > threshold * maxinodes:
            critical_filesets[fs_info['filesetName']] = InodeCritical(used=used, allocated=allocated,
                                                                      maxinodes=maxinodes)

    return critical_filesets
