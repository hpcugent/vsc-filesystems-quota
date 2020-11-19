#!/usr/bin/env python
#
# Copyright 2012-2020 Ghent University
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
Script to check for quota transgressions and notify the offending users.

- relies on mmrepquota to get a quick estimate of user quota
- checks all storage systems that are listed in /etc/quota_check.conf
- writes quota information in gzipped json files in the target directory for the
  affected entity (user, project, vo)
- mails a user, vo or project moderator
- can dump data to the account page (through a REST API) or in files in the user's directories

@author Andy Georges
"""

from vsc.accountpage.client import AccountpageClient
from vsc.config.base import VscStorage, GENT
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.quota.tools import get_mmrepquota_maps, map_uids_to_names
from vsc.filesystem.quota.tools import process_user_quota, process_fileset_quota
from vsc.utils.script_tools import ExtendedSimpleOption

# Constants
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # one hour

QUOTA_USERS_WARNING = 20
QUOTA_USERS_CRITICAL = 40
QUOTA_FILESETS_CRITICAL = 1


def main():
    """Main script"""

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('the VSC filesystems that are checked by this script', None, 'extend', []),
        'write-cache': ('Write the data into the cache files in the FS', None, 'store_true', False),
        'account_page_url': ('Base URL of the account page', None, 'store', 'https://account.vscentrum.be/django'),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
    }
    opts = ExtendedSimpleOption(options)
    logger = opts.log

    try:
        client = AccountpageClient(token=opts.options.access_token)

        user_id_map = map_uids_to_names()  # is this really necessary?
        gpfs = GpfsOperations()
        storage = VscStorage()

        target_filesystems = [storage[s].filesystem for s in opts.options.storage]

        filesystems = gpfs.list_filesystems(device=target_filesystems).keys()
        logger.debug("Found the following GPFS filesystems: %s" % (filesystems))

        filesets = gpfs.list_filesets(devices=target_filesystems)
        logger.debug("Found the following GPFS filesets: %s" % (filesets))

        quota = gpfs.list_quota(devices=target_filesystems)
        exceeding_filesets = {}
        exceeding_users = {}
        stats = {}

        for storage_name in opts.options.storage:

            logger.info("Processing quota for storage_name %s" % (storage_name))
            filesystem = storage[storage_name].filesystem
            replication_factor = storage[storage_name].data_replication_factor

            if filesystem not in filesystems:
                logger.error("Non-existent filesystem %s" % (filesystem))
                continue

            if filesystem not in quota.keys():
                logger.error("No quota defined for storage_name %s [%s]" % (storage_name, filesystem))
                continue

            quota_storage_map = get_mmrepquota_maps(
                quota[filesystem],
                storage_name,
                filesystem,
                filesets,
                replication_factor,
            )

            exceeding_filesets[storage_name] = process_fileset_quota(
                storage, gpfs, storage_name, filesystem, quota_storage_map['FILESET'],
                client, dry_run=opts.options.dry_run, institute=opts.options.host_institute)

            exceeding_users[storage_name] = process_user_quota(
                storage, gpfs, storage_name, None, quota_storage_map['USR'],
                user_id_map, client, dry_run=opts.options.dry_run, institute=opts.options.host_institute)

            stats["%s_fileset_critical" % (storage_name,)] = QUOTA_FILESETS_CRITICAL
            if exceeding_filesets[storage_name]:
                stats["%s_fileset" % (storage_name,)] = 1
                logger.warning("storage_name %s found %d filesets that are exceeding their quota",
                               storage_name, len(exceeding_filesets))
                for (e_fileset, e_quota) in exceeding_filesets[storage_name]:
                    logger.warning("%s has quota %s" % (e_fileset, str(e_quota)))
            else:
                stats["%s_fileset" % (storage_name,)] = 0
                logger.debug("storage_name %s found no filesets that are exceeding their quota" % storage_name)

            stats["%s_users_warning" % (storage_name,)] = QUOTA_USERS_WARNING
            stats["%s_users_critical" % (storage_name,)] = QUOTA_USERS_CRITICAL
            if exceeding_users[storage_name]:
                stats["%s_users" % (storage_name,)] = len(exceeding_users[storage_name])
                logger.warning("storage_name %s found %d users who are exceeding their quota" %
                               (storage_name, len(exceeding_users[storage_name])))
                for (e_user_id, e_quota) in exceeding_users[storage_name]:
                    logger.warning("%s has quota %s" % (e_user_id, str(e_quota)))
            else:
                stats["%s_users" % (storage_name,)] = 0
                logger.debug("storage_name %s found no users who are exceeding their quota" % storage_name)

    except Exception as err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")

    opts.epilogue("quota check completed", stats)


if __name__ == '__main__':
    main()
