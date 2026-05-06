#
# Copyright 2023-2026 Ghent University
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
"""

import logging
import pwd
import re
from collections import defaultdict
from prometheus_client.parser import text_string_to_metric_families

import diskcache as dc
import requests

from vsc.accountpage.client import AccountpageClient
from vsc.config.base import GENT, VO_PREFIX_BY_SITE, VO_SHARED_PREFIX_BY_SITE, VscStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.quota.utils import QUOTA_USER_KIND, QUOTA_VO_KIND, DjangoPusher, UsageInformation
from vsc.utils.script_tools import CLI

DISK_CACHE_LOCATION = "/var/cache/pusage.cache"

KIND_MAP = {
    "user": "USR",
    "fileset": "FILESET",
    "group": "GROUP",
}

SUFFIX_MAP = {
    "in_doubt_bytes": "block_doubt",
    "in_doubt_files": "files_doubt",
    "limit_bytes": "block_hard",
    "limit_files": "files_hard",
    "quota_bytes": "block_soft",
    "quota_files": "files_soft",
    "used_bytes": "block_usage",
    "used_files": "files_usage",
}


def parse_metric_name(name):
    """
    Returns (kind, field) or (None, None) if not a relevant GPFS metric.
    e.g. gpfs_user_used_files -> ("USR", "files_usage")
    """
    parts = name.split("_")
    if parts[0] != "gpfs" or len(parts) < 3:
        return None, None

    kind = KIND_MAP.get(parts[1], None)
    if not kind:
        return None, None

    suffix = "_".join(parts[2:])
    field = SUFFIX_MAP.get(suffix, None)

    return kind, field  # field may be None if suffix is not in SUFFIX_MAP


class UsageReporter(CLI):
    CLI_OPTIONS = {
        "storage": ("the VSC filesystems that are checked by this script", None, "extend", []),
        "account_page_url": ("Base URL of the account page", None, "store", "https://account.vscentrum.be/django"),
        "access_token": ("OAuth2 token to access the account page REST API", None, "store", None),
        "host_institute": ("Name of the institute where this script is being run", str, "store", GENT),
        "metrics_url": ("Endpoint to scrape the metrics from", str, "store", None),
        "metrics_user": ("User to talk to the metric endpoint", str, "store", "prometheus"),
        "metrics_passwd": ("Password for user talking to metrics endpoint", str, "store", None),
        "ca_file": ("CA location", str, "store", "/etc/ipa/ca.crt"),
        "key_file": ("Key location", str, "store", "/etc/ipa/quattor/keys/host.key"),
        "cert_file": ("Cert location", str, "store", "/etc/ipa/quattor/certs/host.pem"),
    }

    def _translate_gpfs(self, entity, kind, fileset, fs):

        if kind == "USR":

            try:
                uid = int(entity)
            except ValueError:
                logging.error("Not a numerical uid for entity: %s", entity)
                return None, None
            try:
                pw_entry = pwd.getpwuid(uid)
                entity = pw_entry.pw_name  # should be the vsc username if it is known to sssd
            except KeyError:
                logging.warning("UID %s not known to sssd, skipping", uid)
                return None, None

            fileset = self.fileset_map[fs][fileset]["filesetName"]

        return entity, fileset

    def process_event(self, event, dry_run):
        if event and event.filesystem in self.system_storage_map.values():
            cache_key = (event.filesystem, event.fileset, event.entity, event.kind)
            cached_usage = self.cache.get(cache_key, default=None)
            if cached_usage == event:
                logging.debug("Event %s equals cached version", event)
            else:
                if not dry_run:
                    self.cache.set(cache_key, event, expire=864000)
                logging.debug("Event %s differs from %s, considering adding to usage list", event, cached_usage)

                # user and fileset may be a number and need translation, but only for the events we will
                # actually upload
                entity, fileset = self._translate_gpfs(event.entity, event.kind, event.fileset, event.filesystem)
                event = event._replace(entity=entity, fileset=fileset)

                if entity is not None and fileset is not None:
                    logging.debug("Adding event %s to usage list", event)
                    self.usage_list.append(event)

    def consolidate(self, entries):
        """
        Assemble a single UsageInformation tuple based on the data coming in for the various
        GPFS  metrics. Each of these comes on a single line.
        """

        grouped = defaultdict(lambda: {"kind": None, "fields": {}})

        for entry in entries:
            name = entry.get("name", "")
            kind, field = parse_metric_name(name)

            if not kind or not field:
                continue

            tags = entry.get("tags", {})
            if not all(k in tags for k in ("fs", "fileset")):
                continue

            # user may not be present for fileset-level metrics
            entity = tags.get("user", None) or tags.get("fileset", None)
            fileset = tags.get("fileset")  # will always pass
            fs = tags["fs"]

            logging.debug("entry data: kind %s - fs %s - fileset %s - entity -%s", kind, fs, fileset, entity)

            if entity is None or fileset is None:
                continue

            key = (fs, fileset, entity, kind)

            grouped[key]["kind"] = kind
            grouped[key]["fields"][field] = entry["value"]

        results = []
        for (fs, fileset, entity, kind), data in grouped.items():
            f = data["fields"]

            # the gpfs exporter provides data in bytes, as opposed to blocks
            usage = UsageInformation(
                filesystem=fs,
                fileset=fileset,
                entity=entity,
                kind=kind,
                block_usage=f.get("block_usage", 0.0) / 1024,
                block_soft=f.get("block_soft", 0.0) / 1024,
                block_hard=f.get("block_hard", 0.0) / 1024,
                block_doubt=f.get("block_doubt", 0.0) / 1024,
                block_expired=(False, 0),
                files_usage=f.get("files_usage", 0.0),
                files_soft=f.get("files_soft", 0.0),
                files_hard=f.get("files_hard", 0.0),
                files_doubt=f.get("files_doubt", 0.0),
                files_expired=(False, 0),
            )
            usage = self._update_usage(usage)
            logging.debug("Appending usage: %s", usage)
            results.append(usage)

        return results

    def scrape_metrics_endpoint(self):

        response = requests.get(
            url=self.options.metrics_url,
            auth=(self.options.metrics_user, self.options.metrics_passwd),
            cert=(self.options.cert_file, self.options.key_file),
            verify=self.options.ca_file,
        )
        response.raise_for_status()

        entries = []
        for family in text_string_to_metric_families(response.text):
            for sample in family.samples:
                # logging.debug("Got data: %s, %s, %s", sample.name, sample.labels, sample.value)
                entries.append({
                    "name": sample.name,
                    "tags": sample.labels,  # already a dict: {"fs": ..., "fileset": ..., "user": ...}
                    "value": sample.value,
                })

        logging.debug("Got %d entries", len(entries))

        consolidated = self.consolidate(entries)
        for c in consolidated:
            self.process_event(c, self.options.dry_run)

    def do(self, dry_run):
        # pylint: disable=unused-argument

        ap_client = AccountpageClient(token=self.options.access_token, url=self.options.account_page_url + "/api/")

        self.storage = VscStorage()
        self.system_storage_map = {k: self.storage[GENT][k].filesystem for k in self.storage if k != GENT}
        self.replication_factors = {
            self.storage[GENT][k].filesystem: self.storage[GENT][k].data_replication_factor
            for k in self.storage
            if k != GENT
        }

        g = GpfsOperations()
        self.fileset_map = g.list_filesets()

        logging.debug("storage map: %s", self.system_storage_map)

        self.usage_list = []
        with dc.Cache(DISK_CACHE_LOCATION) as cache:
            self.cache = cache
            self.scrape_metrics_endpoint()

        for storage_name in self.options.storage:
            logging.info("Processing quota for storage_name %s", storage_name)
            fileset_quota_data = [
                q
                for q in self.usage_list
                if self.system_storage_map[storage_name] == q.filesystem and q.kind == "FILESET"
            ]
            logging.debug("Fileset quota for storage %s: %s", storage_name, fileset_quota_data)
            self.process_fileset_quota(storage_name, fileset_quota_data, ap_client)

            usr_quota_data = [
                q for q in self.usage_list if self.system_storage_map[storage_name] == q.filesystem and q.kind == "USR"
            ]
            logging.info("Usr quota for storage %s: %s", storage_name, usr_quota_data)
            self.process_user_quota(storage_name, usr_quota_data, ap_client)

    def process_user_quota(self, storage_name, quota_list, client):
        institute = self.options.host_institute

        logging.info("Logging user quota to account page")
        logging.debug("Considering the following quota items for pushing: %s", quota_list)

        with DjangoPusher(storage_name, client, QUOTA_USER_KIND, self.options.dry_run) as pusher:
            for quota in quota_list:

                user_name = quota.entity
                fileset_re = (
                    rf"^(vsc[1-5]|{VO_PREFIX_BY_SITE[institute]}|"
                    rf"{VO_SHARED_PREFIX_BY_SITE[institute]})"
                )
                entity_re = r"^vsc[1-5]"

                if re.search(fileset_re, quota.fileset) and re.search(entity_re, quota.entity):
                    logging.debug("Pushing data %s", quota)
                    pusher.push_quota(user_name, quota)

    def process_fileset_quota(self, storage_name, quota_list, client):
        logging.info("Logging VO quota to account page")
        logging.debug("Considering the following quota items for pushing: %s", quota_list)

        institute = self.options.host_institute

        with DjangoPusher(storage_name, client, QUOTA_VO_KIND, self.options.dry_run) as pusher:
            for quota in quota_list:
                fileset_name = quota.fileset

                if not fileset_name.startswith(VO_PREFIX_BY_SITE[institute]):
                    continue
                elif fileset_name.startswith(VO_SHARED_PREFIX_BY_SITE[institute]):
                    vo_name = fileset_name.replace(VO_SHARED_PREFIX_BY_SITE[institute], VO_PREFIX_BY_SITE[institute])
                    shared = True
                else:
                    vo_name = fileset_name
                    shared = False

                logging.debug("Pushing data %s", quota)
                pusher.push_quota(vo_name, quota, shared=shared)

    def _update_usage(self, usage):
        """
        Update the quota information for an entity (user or fileset).
        """

        block_expired = (False, None)
        files_expired = (False, None)

        # when we get data from a filesystem that is not in the
        # config file, we shoud ignore it
        try:
            replication_factor = self.replication_factors[usage.filesystem]
        except KeyError:
            logging.warning(f"Skipping usage for filesystem {usage.filesystem}")
            return None

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
