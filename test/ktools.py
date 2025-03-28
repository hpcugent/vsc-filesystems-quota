#
# Copyright 2023-2025 Ghent University
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

import vsc.filesystem.quota.ktools as tools
import vsc.config.base as config

from vsc.config.base import VSC_DATA, GENT
from vsc.filesystem.quota.ktools import DjangoPusher, determine_grace_period, QUOTA_USER_KIND
from vsc.install.testing import TestCase

config.STORAGE_CONFIGURATION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'filesystem_info.conf')


class TestAuxiliary(TestCase):
    """
    Stuff that does not belong anywhere else :)
    """
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
    @mock.patch('vsc.accountpage.sync.ExtendedSimpleOption.prologue')
    def test_process_user_quota_no_store(self, mock_prologue, mock_django_pusher): # pylint: disable=unused-argument

        storage_name = VSC_DATA
        filesystem = 'kyukondata'
        usage1 = tools.UsageInformation(
            filesystem, 'vsc400', 'vsc40075', "USR", block_usage=1230, block_soft=456, block_hard=789, block_doubt=0,
            block_expired=(False, None), files_usage=100, files_soft=200, files_hard=300, files_doubt=0,
            files_expired=(False, None))
        usage2 = tools.UsageInformation(
            filesystem, 'gvo00002', 'vsc40075', "USR", block_usage=1230, block_soft=456, block_hard=789, block_doubt=0,
            block_expired=(False, None), files_usage=100, files_soft=200, files_hard=300, files_doubt=0,
            files_expired=(False, None))

        client = mock.MagicMock()

        usage_list = [usage1, usage2]

        QR = tools.UsageReporter()
        QR.storage = mock.MagicMock()

        QR.process_user_quota(storage_name, usage_list, client)

        self.assertEqual(mock_django_pusher.call_count, 2)

        mock_django_pusher.assert_has_calls(
            [mock.call('vsc40075', usage1), mock.call('vsc40075', usage2)],
            any_order=True,
        )

    def test_django_pusher(self):

        client = mock.MagicMock()

        with DjangoPusher("my_storage", client, QUOTA_USER_KIND, False) as pusher:
            for i in range(0, 101):
                pusher.push("my_storage", f"pushing {int(i)}")

            self.assertEqual(pusher.payload, {"my_storage": [], "my_storage_SHARED": []})

    def test_django_push_quota(self):

        client = mock.MagicMock()

        quota_info = tools.UsageInformation(
            filesystem="my_storage",
            fileset="vsc100",
            entity="vsc10001",
            kind="USR",
            block_usage=1230,
            block_soft=456,
            block_hard=789,
            block_doubt=0,
            block_expired=(False, None),
            files_usage=130,
            files_soft=380,
            files_hard=560,
            files_doubt=0,
            files_expired=(False, None),
        )

        with DjangoPusher("my_storage", client, QUOTA_USER_KIND, False) as pusher:
            pusher.push_quota('vsc10001', quota_info, shared=False)

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

            pusher.push_quota('vsc10001', quota_info, shared=True)

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
