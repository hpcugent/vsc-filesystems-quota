#
# Copyright 2015-2023 Ghent University
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
Helper functions for all things quota related.

@author: Andy Georges (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

import logging
import re
import time

from pwd import getpwuid, getpwall

from vsc.config.base import GENT, STORAGE_SHARED_SUFFIX, VO_PREFIX_BY_SITE, VO_SHARED_PREFIX_BY_SITE, VSC
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset

QUOTA_USER_KIND = 'user'
QUOTA_VO_KIND = 'vo'


class DjangoPusher(object):
    """Context manager for pushing stuff to django"""

    def __init__(self, storage_name, client, kind, dry_run):
        self.storage_name = storage_name
        self.storage_name_shared = storage_name + STORAGE_SHARED_SUFFIX
        self.client = client
        self.kind = kind
        self.dry_run = dry_run

        self.count = {
            self.storage_name: 0,
            self.storage_name_shared: 0
        }

        self.payload = {
            self.storage_name: [],
            self.storage_name_shared: []
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if self.payload[self.storage_name]:
            self._push(self.storage_name, self.payload[self.storage_name])
        if self.payload[self.storage_name_shared]:
            self._push(self.storage_name_shared, self.payload[self.storage_name_shared])

        if exc_type is not None:
            logging.error("Received exception %s in DjangoPusher: %s", exc_type, exc_value)
            return False

        return True

    def push(self, storage_name, payload):
        if storage_name not in self.payload:
            logging.error("Can not add payload for unknown storage: %s vs %s", storage_name, self.storage_name)
            return

        self.payload[storage_name].append(payload)
        self.count[storage_name] += 1

        if self.count[storage_name] > 100:
            self._push(storage_name, self.payload[storage_name])
            self.count[storage_name] = 0
            self.payload[storage_name] = []

    def push_quota(self, owner, fileset, quota, shared=False):
        """
        Push quota to accountpage: it belongs to owner (can either be user_id or vo_id),
        in the given fileset and quota.
        :param owner: the name of the user or VO to which the quota belongs
        :param fileset: fileset name
        :param quota: actual quota data
        :param shared: is this a shared user/VO quota or not?
        """
        params = {
            "fileset": fileset,
            "used": quota.used,
            "soft": quota.soft,
            "hard": quota.hard,
            "doubt": quota.doubt,
            "expired": quota.expired[0],
            "remaining": quota.expired[1] or 0,  # seconds
            "files_used": quota.files_used,
            "files_soft": quota.files_soft,
            "files_hard": quota.files_hard,
            "files_doubt": quota.files_doubt,
            "files_expired": quota.files_expired[0],
            "files_remaining": quota.files_expired[1] or 0,  # seconds
        }

        if self.kind == QUOTA_USER_KIND:
            params['user'] = owner
        elif self.kind == QUOTA_VO_KIND:
            params['vo'] = owner

        if shared:
            self.push(self.storage_name_shared, params)
        else:
            self.push(self.storage_name, params)

    def _push(self, storage_name, payload):
        """Does the actual pushing to the REST API"""

        if self.dry_run:
            logging.info("Would push payload to account web app: %s", payload)
        else:
            try:
                cl = self.client.usage.storage[storage_name]
                if self.kind == QUOTA_USER_KIND:
                    logging.debug("Pushing user payload to account web app: %s", payload)
                    cl = cl.user
                elif self.kind == QUOTA_VO_KIND:
                    logging.debug("Pushing vo payload to account web app: %s", payload)
                    cl = cl.vo
                else:
                    logging.error("Unknown quota kind, not pushing any quota to the account page")
                    return
                cl.size.put(body=payload)  # if all is well, there's nothing returned except (200, empty string)
            except Exception:
                logging.error("Could not store quota info in account web app")
                raise


def process_user_quota(storage, operator, storage_name, filesystem, quota_map, user_map, client,
                       dry_run=False, institute=GENT):
    """
    Wrapper around the new function to keep the old behaviour intact.
    """
    del filesystem
    del operator

    exceeding_users = []
    path_template = storage.path_templates[institute][storage_name]
    vsc = VSC()

    logging.info("Logging user quota to account page")
    logging.debug("Considering the following quota items for pushing: %s", quota_map)

    with DjangoPusher(storage_name, client, QUOTA_USER_KIND, dry_run) as pusher:
        for (user_id, quota) in quota_map.items():

            user_institute = vsc.user_id_to_institute(int(user_id))
            if user_institute != institute:
                continue

            user_name = user_map.get(int(user_id), None)
            if not user_name:
                try:
                    user_name = getpwuid(int(user_id)).pw_name
                except KeyError:
                    continue

            fileset_name = path_template['user'](user_name)[1]

            fileset_re = '^(vsc[1-4]|%s|%s|%s)' % (VO_PREFIX_BY_SITE[institute],
                                                   VO_SHARED_PREFIX_BY_SITE[institute],
                                                   fileset_name)

            for (fileset, quota_) in quota.quota_map.items():
                if re.search(fileset_re, fileset):
                    pusher.push_quota(user_name, fileset, quota_)

            if quota.exceeds():
                exceeding_users.append((user_name, quota))

    return exceeding_users


def get_quota_maps(storage, operator, storage_name):
    """Obtain the quota information.

    This function uses the storage backend operator to obtain
    quota information for all filesystems known to the storage.

    The returned dictionaries contain all information on a per user
    and per fileset basis for the given filesystem. Users with multiple
    quota settings across different filesets are processed correctly.

    Returns { "USR": user dictionary, "FILESET": fileset dictionary}.
    """
    user_map = {}
    fs_map = {}

    timestamp = int(time.time())

    filesystem = storage[storage_name].filesystem

    quotas = operator().list_quota(devices=filesystem)
    quota_map = quotas[filesystem]

    quota_type = operator().quota_types.USR.value
    logging.info("ordering %s quota for storage %s", quota_type, storage_name)
    # Iterate over a list of named tuples -- StorageQuota
    for (quota_id, storage_quota) in quota_map[quota_type].items():
        user = operator().get_quota_owner(quota_id, filesystem)
        user_quota = user_map.get(user, QuotaUser(storage_name, filesystem, user))
        user_map[user] = _update_quota_entity(
            user_quota,
            storage,
            operator,
            storage_name,
            storage_quota,
            timestamp,
        )

    logging.info("ordering FILESET quota for storage %s", storage)
    # Iterate over a list of named tuples -- StorageQuota
    quota_type = operator().quota_types.FILESET.value
    for (quota_id, storage_quota) in quota_map[quota_type].items():
        fileset_id = operator().get_quota_fileset(quota_id, filesystem)
        fileset_quota = fs_map.get(fileset_id, QuotaFileset(storage_name, filesystem, fileset_id))
        fs_map[fileset_id] = _update_quota_entity(
            fileset_quota,
            storage,
            operator,
            storage_name,
            storage_quota,
            timestamp,
        )

    user_label = operator().quota_types.USR.name
    fileset_label = operator().quota_types.FILESET.name
    return {user_label: user_map, fileset_label: fs_map}


def _update_quota_entity(entity, storage, operator, storage_name, storage_quotas, timestamp):
    """
    Update the quota information for an entity (user or fileset).

    @type entity: QuotaEntity instance
    @type storage: VscStorage object
    @type operator: StorageOperator object
    @type storage_name: string
    @type storage_quota: list of StorageQuota namedtuple instances
    @type timestamp: a timestamp, duh. an integer
    """
    if not isinstance(storage_quotas, list):
        storage_quotas = [storage_quotas]

    filesystem = storage[storage_name].filesystem
    replication_factor = storage[storage_name].data_replication_factor

    for quota in storage_quotas:
        logging.debug("StorageQuota = %s", quota)

        block_expired, files_expired = operator().determine_grace_periods(quota)

        if quota.filesetname:
            # filesetname actually has the fileset ID linked to this fileset/user/group quota
            # convert to its actual fileset name (e.g. filesetName in GPFS)
            fileset_name = operator().get_fileset_name(quota.filesetname, filesystem)
        else:
            fileset_name = None

        logging.debug("The fileset name is %s (filesystem %s); blockgrace %s to expired %s",
                      fileset_name, filesystem, quota.blockGrace, block_expired)

        # XXX: We do NOT divide by the metatadata_replication_factor (yet), since we do not
        #      set the inode quota through the account page. As such, we need to have the exact
        #      usage available for the user -- this is the same data reported in ES by gpfsbeat.
        entity.update(fileset=fileset_name,
                      used=int(quota.blockUsage) // replication_factor,
                      soft=int(quota.blockQuota) // replication_factor,
                      hard=int(quota.blockLimit) // replication_factor,
                      doubt=int(quota.blockInDoubt) // replication_factor,
                      expired=block_expired,
                      files_used=int(quota.filesUsage),
                      files_soft=int(quota.filesQuota),
                      files_hard=int(quota.filesLimit),
                      files_doubt=int(quota.filesInDoubt),
                      files_expired=files_expired,
                      timestamp=timestamp)

    return entity


def process_fileset_quota(storage, operator, storage_name, filesystem, quota_map, client,
                          dry_run=False, institute=GENT):
    """wrapper around the new function to keep the old behaviour intact"""
    del storage

    exceeding_filesets = []

    logging.info("Logging VO quota to account page")
    logging.debug("Considering the following quota items for pushing: %s", quota_map)

    with DjangoPusher(storage_name, client, QUOTA_VO_KIND, dry_run) as pusher:
        for (fileset_id, quota) in quota_map.items():
            fileset_name = operator().get_fileset_name(fileset_id, filesystem)
            logging.debug("Fileset %s quota: %s", fileset_name, quota)

            if not fileset_name.startswith(VO_PREFIX_BY_SITE[institute]):
                continue

            if fileset_name.startswith(VO_SHARED_PREFIX_BY_SITE[institute]):
                vo_name = fileset_name.replace(VO_SHARED_PREFIX_BY_SITE[institute], VO_PREFIX_BY_SITE[institute])
                shared = True
            else:
                vo_name = fileset_name
                shared = False

            for (fileset_, quota_) in quota.quota_map.items():
                pusher.push_quota(vo_name, fileset_, quota_, shared=shared)

            if quota.exceeds():
                exceeding_filesets.append((fileset_name, quota))

    return exceeding_filesets


def map_uids_to_names():
    """Determine the mapping between user ids and user names."""
    ul = getpwall()
    d = {}
    for u in ul:
        d[u[2]] = u[0]
    return d
