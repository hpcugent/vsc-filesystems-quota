#
# Copyright 2023-2023 Ghent University
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

import json
import logging
import re

from collections import namedtuple
from vsc.kafka.cli import ConsumerCLI

from vsc.accountpage.client import AccountpageClient
from vsc.config.base import (
    GENT, STORAGE_SHARED_SUFFIX, VO_PREFIX_BY_SITE, VO_SHARED_PREFIX_BY_SITE,
    VscStorage
)


UsageInformation = namedtuple('UsageInformation', [
    'filesystem', # filesystem name
    'fileset', # fileset (gpfs)
    'entity', # the user or VO owning the usage
    'kind', # the kind of usage info (USR, FILESET, GROUP)
    'block_usage',  # used quota in KiB
    'block_soft',  # soft quota limit in KiB
    'block_hard',  # hard quota limit in KiB
    'block_doubt',  # the KiB GPFS is not sure about
    'block_expired',  # tuple (boolean, grace period expressed in seconds)
    'files_usage',  # used number of inodes
    'files_soft',  # soft limit for inodes
    'files_hard',  # hard limit for inodes
    'files_doubt',  # the inodes GPFS is not sure about
    'files_expired',  # tuple (boolean, grace period expressed in seconds)
])

GPFS_GRACE_REGEX = re.compile(
    r"(?P<days>\d+)\s*days?|(?P<hours>\d+)\s*hours?|(?P<minutes>\d+)\s*minutes?|(?P<expired>expired)"
)

GPFS_NOGRACE_REGEX = re.compile(r"none", re.I)

QUOTA_USER_KIND = 'USR'
QUOTA_VO_KIND = 'FILESET'


class QuotaException(Exception):
    pass


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

    def push_quota(self, owner, quota, shared=False):
        """
        Push quota to accountpage: it belongs to owner (can either be user_id or vo_id),
        in the given fileset and quota.
        :param owner: the name of the user or VO to which the quota belongs
        :param fileset: fileset name
        :param quota: actual quota data
        :param shared: is this a shared user/VO quota or not?
        """
        params = {
            "fileset": quota.fileset,
            "used": quota.block_usage,
            "soft": quota.block_soft,
            "hard": quota.block_hard,
            "doubt": quota.block_doubt,
            "expired": quota.block_expired[0],
            "remaining": quota.block_expired[1] or 0,  # seconds
            "files_used": quota.files_usage,
            "files_soft": quota.files_soft,
            "files_hard": quota.files_hard,
            "files_doubt": quota.files_doubt,
            "files_expired": quota.files_expired[0],
            "files_remaining": quota.files_expired[1] or 0,  # seconds
        }
        logging.debug("Pushing quota %s with params %s", quota, params)

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


class UsageReporter(ConsumerCLI):

    CLI_OPTIONS = {
        'storage': ('the VSC filesystems that are checked by this script', None, 'extend', []),
        'account_page_url': ('Base URL of the account page', None, 'store', 'https://account.vscentrum.be/django'),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
        'group': ("Kafka consumer group", None, "store", "ap-quota"),
    }

    def convert_msg(self, msg):
        """
        Process msg as JSON.
        Return None on failure or if the message holds no usage information.

        full message looks like:
        {
              "@timestamp": "2023-01-09T19:19:19.518Z",
              "@metadata": {
                "beat": "gpfsbeat",
                "type": "_doc",
                "version": "7.10.0"
              },
              "quota": {
                "files_soft": 0,
                "kind": "USR",
                "files_usage": 2,
                "block_usage": 0,
                "filesystem": "arcaninescratch",
                "entity": "vsc40075",
                "block_hard": 1048576,
                "files_expired": "none",
                "fileset": "gvo00002",
                "block_soft": 995328,
                "files_hard": 0,
                "block_expired": "none",
                "block_doubt": 0,
                "files_doubt": 0
              },
              "type": "gpfsbeat",
              "counter": 657,
              "ecs": {
                "version": "1.6.0"
              },
              "host": {
                "name": "gpfsbeat"
              },
              "agent": {
                "ephemeral_id": "snip",
                "id": "snip",
                "name": "gpfsbeat",
                "type": "gpfsbeat",
                "version": "7.10.0",
                "hostname": "myhost.mydomain"
              }
        }
        """
        value = msg.value
        if value:
            try:
                event = json.loads(value)
            except ValueError:
                logging.error("Failed to load as JSON: %s", value)
                return None

            if 'quota' in event:
                kwargs = dict([(field, event['quota'][field]) for field in UsageInformation._fields])
                return self._update_usage(UsageInformation(**kwargs))
            else:
                return None
        else:
            logging.error("msg has no value %s (%s)", msg, type(msg))
            return None

    def process_event(self, event, dry_run):

        if event and event.filesystem in self.system_storage_map:
            self.quota_list.append(event)


    def do(self, dry_run):
        # pylint: disable=unused-argument

        ap_client = AccountpageClient(token=self.options.access_token)

        self.storage = VscStorage()
        self.system_storage_map = dict([(self.storage[GENT][k].filesystem, k) for k in self.storage if k != GENT])

        logging.info("storage map: %s", self.system_storage_map )

        self.quota_list = []
        super(UsageReporter, self).do(dry_run)

        for storage_name in self.options.storage:

            logging.info("Processing quota for storage_name %s", storage_name)

            fileset_quota_data = [
                q for q in self.quota_list
                if self.system_storage_map[q.filesystem] == storage_name and q.kind == 'FILESET'
            ]
            logging.debug("Fileset quota for storage %s: %s", storage_name, fileset_quota_data)
            self.process_fileset_quota(storage_name, fileset_quota_data, ap_client)

            usr_quota_data = [
                q for q in self.quota_list
                if self.system_storage_map[q.filesystem] == storage_name and q.kind == 'USR'
            ]
            logging.debug("Usr quota for storage %s: %s", storage_name, usr_quota_data)
            self.process_user_quota(storage_name, usr_quota_data, ap_client)

    def process_user_quota(self, storage_name, quota_list, client):

        institute = self.options.host_institute
        path_template = self.storage.path_templates[institute][storage_name]

        logging.info("Logging user quota to account page")
        logging.debug("Considering the following quota items for pushing: %s", quota_list)

        with DjangoPusher(storage_name, client, QUOTA_USER_KIND, self.options.dry_run) as pusher:
            for quota in quota_list:


                if not quota.entity.startswith('vsc'):
                    # no longer a known user, we got the numerical UID, so no need to push info
                    continue

                user_name = quota.entity
                fileset_name = path_template['user'](user_name)[1]
                fileset_re = '^(vsc[1-4]|%s|%s|%s)' % (VO_PREFIX_BY_SITE[institute],
                                                    VO_SHARED_PREFIX_BY_SITE[institute],
                                                    fileset_name)

                if re.search(fileset_re, quota.fileset):
                    pusher.push_quota(user_name, quota)

    def process_fileset_quota(self, storage_name, quota_list, client):

        logging.info("Logging VO quota to account page")
        logging.debug("Considering the following quota items for pushing: %s", quota_list)

        institute = self.options.host_institute

        with DjangoPusher(storage_name, client, QUOTA_VO_KIND, self.options.dry_run) as pusher:
            for quota in quota_list:
                fileset_name = quota.fileset
                logging.debug("Fileset %s quota: %s", fileset_name, quota)

                if not fileset_name.startswith(VO_PREFIX_BY_SITE[institute]):
                    continue
                elif fileset_name.startswith(VO_SHARED_PREFIX_BY_SITE[institute]):
                    vo_name = fileset_name.replace(VO_SHARED_PREFIX_BY_SITE[institute], VO_PREFIX_BY_SITE[institute])
                    shared = True
                else:
                    vo_name = fileset_name
                    shared = False

                pusher.push_quota(vo_name, quota, shared=shared)

    def _update_usage(self, usage):
        """
        Update the quota information for an entity (user or fileset).
        """

        block_expired = determine_grace_period(usage.block_expired)
        files_expired = determine_grace_period(usage.files_expired)

        replication_factor = self.storage[self.system_storage_map[usage.filesystem]].data_replication_factor
        # TODO: check if we should address the inode usage in relation to the replication factor (ideally: no)
        usage = usage._replace(
            block_usage=int(usage.block_usage) // replication_factor,
            block_soft=int(usage.block_soft) // replication_factor,
            block_hard=int(usage.block_hard) // replication_factor,
            block_doubt=int(usage.block_doubt) // replication_factor,
            block_expired=block_expired,
            files_usage=int(usage.files_usage),
            files_soft=int(usage.files_soft),
            files_hard=int(usage.files_hard),
            files_doubt=int(usage.files_doubt),
            files_expired=files_expired,
        )

        logging.debug("Usage after replace: %s", usage)
        return usage


# TODO: move this into gpfsbeat
def determine_grace_period(grace_string):
    grace = GPFS_GRACE_REGEX.search(grace_string)
    nograce = GPFS_NOGRACE_REGEX.search(grace_string)

    if nograce:
        expired = (False, None)
    elif grace:
        grace = grace.groupdict()
        grace_time = 0
        if grace['days']:
            grace_time = int(grace['days']) * 86400
        elif grace['hours']:
            grace_time = int(grace['hours']) * 3600
        elif grace['minutes']:
            grace_time = int(grace['minutes']) * 60
        elif grace['expired']:
            grace_time = 0
        else:
            logging.error("Unprocessed grace groupdict %s (from string %s).",
                          grace, grace_string)
            raise QuotaException("Cannot process grace time string")
        expired = (True, grace_time)
    else:
        logging.error("Unknown grace string %s.", grace_string)
        raise QuotaException("Cannot process grace information (%s)" % grace_string)

    return expired
