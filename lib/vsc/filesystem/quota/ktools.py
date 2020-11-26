#
# Copyright 2020-2020 Ghent University
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
Tools to sync quota from kafka to the AP

Data was pushed to kafka through gpfsbeat. This is then fetched from kafka and
pushed to the AP.

@author: Andy Georges (Ghent University)
"""

import logging
import pwd
import re

from collections import defaultdict, namedtuple
from kafka import KafkaConsumer

from vsc.accountpage.client import AccountpageClient
from vsc.config.base import STORAGE_SHARED_SUFFIX, VscStorage, VSC, GENT
from vsc.config.base import VO_PREFIX_BY_SITE, VO_SHARED_PREFIX_BY_SITE
from vsc.filesystem.quota.tools import QuotaException
from vsc.utils.script_tools import NrpeCLI

from enum import Enum
import json

GPFS_GRACE_REGEX = re.compile(
    r"(?P<days>\d+)\s*days?|(?P<hours>\d+)\s*hours?|(?P<minutes>\d+)\s*minutes?|(?P<expired>expired)"
)

GPFS_NOGRACE_REGEX = re.compile(r"none", re.I)

class UsageType(Enum):
    UserUsage = "USR"
    FilesetUsage = "FILESET"


UsageInformation = namedtuple('UsageInformation', [
    'used',  # used quota in KiB
    'soft',  # soft quota limit in KiB
    'hard',  # hard quota limit in KiB
    'doubt',  # the KiB GPFS is not sure about
    'expired',  # tuple (boolean, grace period expressed in seconds)
    'files_used',  # used number of inodes
    'files_soft',  # soft limit for inodes
    'files_hard',  # hard limit for inodes
    'files_doubt',  # the inodes GPFS is not sure about
    'files_expired',  # tuple (boolean, grace period expressed in seconds)
])


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

        if self.kind == UsageType.UserUsage:
            params['user'] = owner
        elif self.kind == UsageType.FilesetUsage:
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
                if self.kind == UsageType.UserUsage:
                    logging.debug("Pushing user payload to account web app: %s", payload)
                    cl = cl.user
                elif self.kind == UsageType.FilesetUsage:
                    logging.debug("Pushing vo payload to account web app: %s", payload)
                    cl = cl.vo
                else:
                    logging.error("Unknown quota kind, not pushing any quota to the account page")
                    return
                cl.size.put(body=payload)  # if all is well, there's nothing returned except (200, empty string)
            except Exception:
                logging.error("Could not store quota info in account web app")
                raise


def map_uids_to_names():
    """Determine the mapping between user ids and user names."""
    ul = pwd.getpwall()
    d = {}
    for u in ul:
        d[u[2]] = u[0]
    return d


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


def process_user_quota(vsc, pusher, quota, user_map, institute=GENT, dry_run=False):

    logging.info("Logging user quota to account page")
    logging.debug("Considering the following quota items for pushing: %s", quota)

    for (user_id, fileset, quota) in quota:

        user_institute = vsc.user_id_to_institute(int(user_id))
        if user_institute != institute:
            logging.info("Wrong institute")
            continue

        user_name = user_map.get(int(user_id), None)
        if not user_name:
            logging.info("Username not found")
            continue

        fileset_re = '^(vsc[1-4]|%s|%s)' % (
            VO_PREFIX_BY_SITE[institute],
            VO_SHARED_PREFIX_BY_SITE[institute])

        logging.info("Boe")

        if re.search(fileset_re, fileset):
            pusher.push_quota(user_name, fileset, quota)
        else:
            logging.info("fileset does not match regex")

class QuotaSync(NrpeCLI):

    CLI_OPTIONS = {
        'account_page_url': ('Base URL of the account page', None, 'store', 'https://account.vscentrum.be/django'),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'host_institute': ('Name of the institute where this script is being run', str, 'store', GENT),
        'brokers': ("List of kafka brokers, comma separated", str, "store", None),
        'topic': ("Kafka topic to publish to", str, "store", "xdmod"),
        'produce': ("Produce data to Kafka", None, "store_true", False),
        'consume': ("Consume data from Kafka", None, "store_true", False),
        'consumer_group': ("Kafka consumer group", str, "store", "ap-quota-sync"),
        'timeout': ('Kafka consumer timeout in ms. If not set, loops forever', int, "store", None),
        'security_protocol': ("Security protocol to use, e.g., SASL_SSL", str, "store", "PLAINTEXT"),
        'ssl': ("Comma-separated key=value list of SSL options for underlying kafka lib", "strlist", "store", []),
        'sasl': ("Comma-separated key=value list of SASL options for the underlying kafka lib", "strlist", "store", []),
    }

    def get_kafka_args(self):

        return dict(map(lambda kv: kv.split('='), self.options.ssl + self.options.sasl))

    def consume(self, fsmap, storage, dry_run):
        """Consume job info from kafka"""

        kwargs = self.get_kafka_args()
        if self.options.timeout is not None:
            kwargs["consumer_timeout_ms"] = self.options.timeout

        consumer = KafkaConsumer(
            self.options.topic,
            bootstrap_servers=self.options.brokers,
            group_id=self.options.consumer_group,
            security_protocol=self.options.security_protocol,
            **kwargs
        )

        processed_quota = {
            UsageType.UserUsage: defaultdict(list),
            UsageType.FilesetUsage: defaultdict(list),
        }

        for msg in consumer:

            if not msg.value:
                continue

            info = json.loads(msg.value)

            # check if this is an actual mmrepquota message
            if "quota" not in info:
                continue

            quota = info["quota"]
            storage_name = fsmap[quota["filesystem"]]
            replication_factor = storage[storage_name].data_replication_factor
            block_expired = determine_grace_period(quota["expired"])
            files_expired = determine_grace_period(quota["files_expired"])

            usage = UsageInformation(
                used=quota["block_usage"] // replication_factor,
                soft=quota["block_soft"] // replication_factor,
                hard=quota["block_hard"] // replication_factor,
                doubt=quota["block_doubt"] // replication_factor,
                expired=block_expired,
                files_used=quota["files_usage"],
                files_soft=quota["files_soft"],
                files_hard=quota["files_hard"],
                files_doubt=quota["files_doubt"],
                files_expired=files_expired,
            )
            try:
                processed_quota[quota["kind"]][storage_name].append((
                    quota["entity"],
                    quota["fileset"],
                    usage
                ))
            except KeyError:
                # we only care about USR and FILESET quota
                pass

            consumer.commit()  # this is essentially one past the post ack, but we already have that message as well

        logging.info("All messages retrieved")
        return processed_quota

    def do(self, dry_run):

        client = AccountpageClient(token=self.opts.options.access_token)

        vsc = VSC()
        vsc_storage = VscStorage()
        fsmap = dict([(vsc_storage[k].filesystem, k) for k in vsc_storage.keys() if k.startswith('VSC')])

        user_id_map = map_uids_to_names()  # is this really necessary?
        storage = VscStorage()

        processed_quota = self.consume(fsmap, storage, dry_run)

        for storage_name in self.opts.options.storage:

            if storage_name in processed_quota[UsageType.UserUsage]:
                with DjangoPusher(storage_name, client, UsageType.UserUsage, dry_run) as pusher:
                    process_user_quota(vsc, pusher, processed_quota[UsageType.UserUsage][storage_name], user_id_map)

            if storage_name in processed_quota[UsageType.FilesetUsage]:
                with DjangoPusher(storage_name, client, UsageType.FilesetUsage, dry_run) as pusher:
                    pass


