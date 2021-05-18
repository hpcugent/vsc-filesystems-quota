#
# Copyright 2012-2021 Ghent University
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
import mock

import vsc.filesystem.quota.tools as tools
import vsc.config.base as config

from vsc.config.base import VSC_DATA, GENT
from vsc.filesystem.quota.entities import QuotaUser, QuotaFileset, QuotaInformation
from vsc.filesystem.quota.tools import DjangoPusher, determine_grace_period, QUOTA_USER_KIND
from vsc.install.testing import TestCase

config.STORAGE_CONFIGURATION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'filesystem_info.conf')


class TestAuxiliary(TestCase):
    """
    Stuff that does not belong anywhere else :)
    """
    @mock.patch('vsc.filesystem.quota.tools.getpwall')
    def test_map_uids_to_names(self, mock_getpwall):
        """
        Check that the remapping functions properly
        """
        uids = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]

        mock_getpwall.return_value = uids
        res = tools.map_uids_to_names()

        self.assertEqual(res, {3: 1, 6: 4, 9: 7})

    def test_determine_grace_period(self):
        """
        Check the determine_grace_period function
        """
        self.assertEqual(determine_grace_period("6 days"), (True, 6 * 86400))
        self.assertEqual(determine_grace_period("2 hours"), (True, 2 * 3600))
        self.assertEqual(determine_grace_period("13 minutes"), (True, 13 * 60))
        self.assertEqual(determine_grace_period("expired"), (True, 0))
        self.assertEqual(determine_grace_period("none"), (False, None))


class TestProcessing(TestCase):

    @mock.patch.object(DjangoPusher, 'push_quota')
    @mock.patch('vsc.filesystem.quota.tools.getpwuid')
    def test_process_user_quota_no_store(self, mock_getpwuid, mock_django_pusher):

        storage_name = VSC_DATA
        item = 'vsc40075'
        filesystem = 'kyukondata'
        quota = QuotaUser(storage_name, filesystem, item)
        quota.update('vsc400', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        storage = config.VscStorage()

        client = mock.MagicMock()

        quota_map = {
            '2540075': quota,
            '2510042': None,  # should be skipped
        }
        user_map = {
            2540075: 'vsc40075',
            2510042: 'vsc10042',
        }

        mock_getpwuid.side_effect=lambda k: user_map.get(k, None)

        tools.process_user_quota(
            storage, None, storage_name, None, quota_map, user_map, client, dry_run=False, institute=GENT
        )

        self.assertEqual(mock_django_pusher.call_count, 2)

        mock_django_pusher.assert_has_calls(
            [mock.call('vsc40075', fileset, quota.quota_map[fileset]) for fileset in ['gvo00002', 'vsc400']],
            any_order=True,
        )

    @mock.patch.object(DjangoPusher, 'push_quota')
    def test_process_fileset_quota_no_store(self, mock_django_pusher):

        storage_name = VSC_DATA
        filesystem = 'vulpixdata'
        fileset = 'gvo00002'
        quota = QuotaFileset(storage_name, filesystem, fileset)
        quota.update('gvo00002', used=1230, soft=456, hard=789, doubt=0, expired=(False, None), timestamp=None)

        storage = mock.MagicMock()

        filesets = {
            filesystem: {
                fileset: {
                    'path': '/my_path',
                    'filesetName': fileset,
                }
            }
        }
        gpfs = mock.MagicMock()
        gpfs.list_filesets.return_value = filesets

        client = mock.MagicMock()

        quota_map = {fileset: quota}

        tools.process_fileset_quota(
            storage, gpfs, storage_name, filesystem, quota_map, client, dry_run=False, institute=GENT
        )

        mock_django_pusher.assert_called_once_with('gvo00002', 'gvo00002', quota.quota_map['gvo00002'], shared=False)

    def test_django_pusher(self):

        client = mock.MagicMock()

        with DjangoPusher("my_storage", client, QUOTA_USER_KIND, False) as pusher:
            for i in range(0, 101):
                pusher.push("my_storage", "pushing %d" % i)

            self.assertEqual(pusher.payload, {"my_storage": [], "my_storage_SHARED": []})

    def test_django_push_quota(self):

        client = mock.MagicMock()

        quota_info = QuotaInformation(
            timestamp=None,
            used=1230,
            soft=456,
            hard=789,
            doubt=0,
            expired=(False, None),
            files_used=130,
            files_soft=380,
            files_hard=560,
            files_doubt=0,
            files_expired=(False, None),
        )

        with DjangoPusher("my_storage", client, QUOTA_USER_KIND, False) as pusher:
            pusher.push_quota('vsc10001', 'vsc100', quota_info, shared=False)

            self.assertEqual(pusher.payload["my_storage"][0], {
                'fileset': 'vsc100',
                'used': 1230,
                'soft': 456,
                'hard': 789,
                'doubt': 0,
                'expired': False,
                'remaining': 0,
                'files_used': 130,
                'files_soft': 380,
                'files_hard': 560,
                'files_doubt': 0,
                'files_expired': False,
                'files_remaining': 0,
                'user': 'vsc10001',
            })

            self.assertEqual(pusher.payload["my_storage_SHARED"], [])

            pusher.push_quota('vsc10001', 'vsc100', quota_info, shared=True)

            self.assertEqual(pusher.payload["my_storage_SHARED"][0], {
                'fileset': 'vsc100',
                'used': 1230,
                'soft': 456,
                'hard': 789,
                'doubt': 0,
                'expired': False,
                'remaining': 0,
                'files_used': 130,
                'files_soft': 380,
                'files_hard': 560,
                'files_doubt': 0,
                'files_expired': False,
                'files_remaining': 0,
                'user': 'vsc10001',
            })
