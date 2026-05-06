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
Tests for all helper functions in vsc.filesystems.quota.tools.

@author: Andy Georges (Ghent University)
@author: Ward Poelmans (Vrije Universiteit Brussel)
"""

import os
import pwd
import vsc.config.base as config
from unittest.mock import MagicMock, patch
from vsc.install.testing import TestCase


from collections import namedtuple
from unittest.mock import MagicMock, patch, PropertyMock

from vsc.filesystem.quota.ptools import (
    parse_metric_name,
    KIND_MAP,
    SUFFIX_MAP,
    UsageReporter,
)
from vsc.filesystem.quota.utils import UsageInformation

config.STORAGE_CONFIGURATION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'filesystem_info.conf')


class TestParseMetricName(TestCase):
    def test_user_used_files(self):
        kind, field = parse_metric_name("gpfs_user_used_files")
        assert kind == "USR"
        assert field == "files_usage"

    def test_fileset_used_blocks(self):
        kind, field = parse_metric_name("gpfs_fileset_used_bytes")
        assert kind == "FILESET"
        assert field == "block_usage"

    def test_group_kind(self):
        kind, field = parse_metric_name("gpfs_group_used_files")
        assert kind == "GROUP"
        assert field == "files_usage"

    def test_not_gpfs_prefix(self):
        kind, field = parse_metric_name("process_cpu_seconds_total")
        assert kind is None
        assert field is None

    def test_unknown_kind(self):
        kind, field = parse_metric_name("gpfs_banana_used_files")
        assert kind is None
        assert field is None

    def test_unknown_suffix(self):
        kind, field = parse_metric_name("gpfs_user_something_weird")
        kind_result, field_result = parse_metric_name("gpfs_user_something_weird")
        assert kind_result == "USR"
        assert field_result is None

    def test_too_short(self):
        kind, field = parse_metric_name("gpfs_user")
        assert kind is None
        assert field is None

    def test_empty_string(self):
        kind, field = parse_metric_name("")
        assert kind is None
        assert field is None


def make_reporter(filesystems=None, replication_factors=None):
    """Build a minimal UsageReporter without going through CLI.__init__."""
    reporter = UsageReporter.__new__(UsageReporter)
    reporter.usage_list = []
    reporter.cache = {}
    reporter.system_storage_map = filesystems or {"home": "kyukonhome"}
    reporter.replication_factors = replication_factors or {"kyukonhome": 1}
    reporter.options = MagicMock()
    reporter.options.metrics_url = "https://ces06.gastly.os:9100/metrics"
    reporter.options.metrics_user = "prometheus"
    reporter.options.metrics_passwd = "secret"
    reporter.options.cert_file = "/etc/ipa/quattor/certs/host.pem"
    reporter.options.key_file = "/etc/ipa/quattor/keys/host.key"
    reporter.options.ca_file = "/etc/ipa/ca.crt"
    reporter.fileset_map = {
        "kyukonhome": { "99": { "filesetName": "gvo00002"}},
    }
    return reporter

def make_entry(name, fs, fileset, user=None, value=0.0):
    tags = {"fs": fs, "fileset": fileset}
    if user:
        tags["user"] = user
    return {
        "name": name,
        "tags": tags,
        "timestamp": "2026-03-25T08:52:36.790027879Z",
        "kind": "USR",
        "value": value,
    }


class TestConsolidate(TestCase):

    def test_single_user_metric(self):
        reporter = make_reporter()
        entries = [make_entry("gpfs_user_used_files", "kyukonhome", "99", "2540001", 100.0)]
        results = reporter.consolidate(entries)
        assert len(results) == 1
        assert results[0].files_usage == 100
        assert results[0].kind == "USR"
        assert results[0].entity == "2540001"

    def test_multiple_metrics_same_user_consolidated(self):
        reporter = make_reporter()
        entries = [
            make_entry("gpfs_user_used_files", "kyukonhome", "99", "2540001", 100.0),
            make_entry("gpfs_user_used_bytes", "kyukonhome", "99", "2540001", 2048.0),
        ]
        results = reporter.consolidate(entries)
        assert len(results) == 1
        assert results[0].files_usage == 100
        assert results[0].block_usage == 2




class TestTranslateGpfs(TestCase):
    def test_usr_known_uid(self):
        reporter = make_reporter()
        reporter.fileset_map = {"kyukonhome": {"99": {"filesetName": "gvo00001"}}}

        mock_pw = MagicMock()
        mock_pw.pw_name = "vsc40001"

        with patch("vsc.filesystem.quota.ptools.pwd.getpwuid", return_value=mock_pw):
            entity, fileset = reporter._translate_gpfs("2540001", "USR", "99", "kyukonhome")

        assert entity == "vsc40001"
        assert fileset == "gvo00001"

    def test_usr_unknown_uid(self):
        reporter = make_reporter()
        reporter.fileset_map = {}

        with patch("vsc.filesystem.quota.ptools.pwd.getpwuid", side_effect=KeyError):
            entity, fileset = reporter._translate_gpfs("9999999", "USR", "99", "kyukonhome")

        assert entity is None
        assert fileset is None

    def test_usr_non_numeric_entity(self):
        reporter = make_reporter()
        reporter.fileset_map = {}

        entity, fileset = reporter._translate_gpfs("notanumber", "USR", "99", "kyukonhome")

        assert entity is None
        assert fileset is None

    def test_fileset_kind_skips_pwd_lookup(self):
        reporter = make_reporter()
        reporter.fileset_map = {}

        with patch("vsc.filesystem.quota.ptools.pwd.getpwuid") as mock_pwd:
            entity, fileset = reporter._translate_gpfs("gvo00001", "FILESET", "gvo00001", "kyukonhome")

        mock_pwd.assert_not_called()
        assert entity == "gvo00001"
        assert fileset == "gvo00001"


class TestProcessEvent(TestCase):
    def _make_usage(self, filesystem="kyukonhome", fileset="99", entity="2540001", kind="USR"):
        return UsageInformation(
            filesystem=filesystem, fileset=fileset, entity=entity, kind=kind,
            block_usage=100, block_soft=200, block_hard=300, block_doubt=0,
            block_expired=(False, None), files_usage=10, files_soft=20, files_hard=30,
            files_doubt=0, files_expired=(False, None),
        )

    def _make_reporter_with_cache(self, cached_value=None):
        reporter = make_reporter()
        reporter.fileset_map = {"kyukonhome": {"99": {"filesetName": "gvo00001"}}}
        mock_cache = MagicMock()
        mock_cache.get.return_value = cached_value
        reporter.cache = mock_cache
        return reporter

    def test_usr_new_event_translated_and_added(self):
        reporter = self._make_reporter_with_cache(cached_value=None)
        event = self._make_usage()

        mock_pw = MagicMock()
        mock_pw.pw_name = "vsc40001"

        with patch("vsc.filesystem.quota.ptools.pwd.getpwuid", return_value=mock_pw):
            reporter.process_event(event, dry_run=False)

        assert len(reporter.usage_list) == 1
        assert reporter.usage_list[0].entity == "vsc40001"
        assert reporter.usage_list[0].fileset == "gvo00001"

    def test_usr_cached_unchanged_event_not_added(self):
        event = self._make_usage()
        reporter = self._make_reporter_with_cache(cached_value=event)

        reporter.process_event(event, dry_run=False)

        assert reporter.usage_list == []

    def test_usr_unknown_uid_not_added(self):
        reporter = self._make_reporter_with_cache(cached_value=None)
        event = self._make_usage()

        with patch("vsc.filesystem.quota.ptools.pwd.getpwuid", side_effect=KeyError):
            reporter.process_event(event, dry_run=False)

        assert reporter.usage_list == []

    def test_fileset_new_event_added_without_translation(self):
        reporter = self._make_reporter_with_cache(cached_value=None)
        event = self._make_usage(entity="gvo00001", kind="FILESET", fileset="gvo00001")

        reporter.process_event(event, dry_run=False)

        assert len(reporter.usage_list) == 1
        assert reporter.usage_list[0].entity == "gvo00001"

    def test_fileset_cached_unchanged_event_not_added(self):
        event = self._make_usage(entity="gvo00001", kind="FILESET", fileset="gvo00001")
        reporter = self._make_reporter_with_cache(cached_value=event)

        reporter.process_event(event, dry_run=False)

        assert reporter.usage_list == []

    def test_cache_not_updated_on_dry_run(self):
        reporter = self._make_reporter_with_cache(cached_value=None)
        event = self._make_usage(entity="gvo00001", kind="FILESET", fileset="gvo00001")

        reporter.process_event(event, dry_run=True)

        reporter.cache.set.assert_not_called()

    def test_cache_updated_when_not_dry_run(self):
        reporter = self._make_reporter_with_cache(cached_value=None)
        event = self._make_usage(entity="gvo00001", kind="FILESET", fileset="gvo00001")

        reporter.process_event(event, dry_run=False)

        reporter.cache.set.assert_called_once()

    def test_event_not_in_storage_map_ignored(self):
        reporter = self._make_reporter_with_cache(cached_value=None)
        event = self._make_usage(filesystem="unknownfs")

        reporter.process_event(event, dry_run=False)

        assert reporter.usage_list == []

    def test_none_event_ignored(self):
        reporter = self._make_reporter_with_cache(cached_value=None)
        reporter.process_event(None, dry_run=False)
        assert reporter.usage_list == []
