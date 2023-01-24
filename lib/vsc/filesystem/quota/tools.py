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

import gzip
import json
import logging
import os
import re
import socket
import time

from collections import namedtuple
from pwd import getpwuid, getpwall
from vsc.utils.script_tools import CLI
from vsc.reporting.xdmod.consumer import ConsumerCLI

from vsc.accountpage.client import AccountpageClient
from vsc.config.base import (
    GENT, STORAGE_SHARED_SUFFIX, VO_PREFIX_BY_SITE, VO_SHARED_PREFIX_BY_SITE,
    VSC, INSTITUTE_ADMIN_EMAIL, VscStorage
)
from vsc.utils.mail import VscMail


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

QUOTA_USER_KIND = 'user'
QUOTA_VO_KIND = 'vo'

NAGIOS_CHECK_INTERVAL_THRESHOLD = (6 * 60 + 5) * 60  # 365 minutes -- little over 6 hours.
INODE_LOG_ZIP_PATH = '/var/log/quota/inode-zips'
INODE_STORE_LOG_CRITICAL = 1

class QuotaException(Exception):
    pass


InodeCritical = namedtuple("InodeCritical", ['used', 'allocated', 'maxinodes'])


CRITICAL_INODE_COUNT_MESSAGE = """
Dear HPC admins,

The following filesets will be running out of inodes soon (or may already have run out).

%(fileset_info)s

Kind regards,
Your friendly inode-watching script
"""


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


class QuotaReporter(ConsumerCLI):

    CLI_OPTIONS = {
        'storage': ('the VSC filesystems that are checked by this script', None, 'extend', []),
        'account_page_url': ('Base URL of the account page', None, 'store', 'https://account.vscentrum.be/django'),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
        'group': ("Kafka consumer group", None, "store", "ap-quota"),
    }

    def process_msg(self, msg):
        """
        Process msg as JSON.
        Return None on failure.

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

    def do(self, dry_run):

        ap_client = AccountpageClient(token=self.options.access_token)

        user_id_map = map_uids_to_names()
        self.storage = VscStorage()

        self.system_storage_map = dict([(self.storage[GENT][k].filesystem, k) for k in self.storage if k != GENT])

        consumer = self.make_consumer(self.options.group)

        def consumer_close():
            # default is autocommit=True, which is not ok wrt dry_run
            consumer.close(autocommit=False)

        quota_list = []

        i = 0  # testing purposes
        for msg in consumer:

            usage = self.process_msg(msg)
            logging.info("Received payload: %s", usage)

            if usage and usage.filesystem in self.system_storage_map:
                quota_list.append(usage)

            i += 1
            if i > 10:
                break

        for storage_name in self.options.storage:

            logging.info("Processing quota for storage_name %s", storage_name)

            fileset_quota_data = [q for q in quota_list if q.filesystem == storage_name and q.kind == 'FILESET']
            self.process_fileset_quota(storage_name, fileset_quota_data, ap_client)

            usr_quota_data = [q for q in quota_list if q.filesystem == storage_name and q.kind == 'USR']
            self.process_user_quota(storage_name, usr_quota_data, user_id_map, ap_client)

    def process_user_quota(self, storage_name, quota_list, user_map, client):

        institute = self.options.host_institute
        path_template = self.storage.path_templates[institute][storage_name]
        vsc = VSC()

        logging.info("Logging user quota to account page")
        logging.debug("Considering the following quota items for pushing: %s", quota_list)

        with DjangoPusher(storage_name, client, QUOTA_USER_KIND, self.options.dry_run) as pusher:
            for quota in quota_list:

                user_id = quota.entity

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

                if re.search(fileset_re, quota.fileset):
                    pusher.push_quota(user_name, quota.fileset, quota)

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

                if fileset_name.startswith(VO_SHARED_PREFIX_BY_SITE[institute]):
                    vo_name = fileset_name.replace(VO_SHARED_PREFIX_BY_SITE[institute], VO_PREFIX_BY_SITE[institute])
                    shared = True
                else:
                    vo_name = fileset_name
                    shared = False

                fileset_ = quota.entity
                pusher.push_quota(vo_name, fileset_, quota, shared=shared)

    def _update_usage(self, usage):
        """
        Update the quota information for an entity (user or fileset).
        """

        block_expired = determine_grace_period(usage.block_expired)
        files_expired = determine_grace_period(usage.files_expired)

        replication_factor = self.storage[self.system_storage_map[usage.filesystem]].data_replication_factor
        # TODO: check if we should address the inode usage in relation to the replication factor (ideally: no)
        usage._replace(
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

        return usage



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



def map_uids_to_names():
    """Determine the mapping between user ids and user names."""
    ul = getpwall()
    d = {}
    for u in ul:
        d[u[2]] = u[0]
    return d


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


class InodeLog(CLI):


    # Note: debug option is provided by generaloption
    # Note: other settings, e.g., ofr each cluster will be obtained from the configuration file
    CLI_OPTIONS = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'location': ('path to store the gzipped files', None, 'store', INODE_LOG_ZIP_PATH),
        'backend': ('Storage backend', None, 'store', 'gpfs'),
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

        backend = self.options.backend
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
            self.mail_admins(
                critical_filesets,
                dry_run=self.options.dry_run,
                host_institute=self.options.host_institute
            )

