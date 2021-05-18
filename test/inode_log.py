#
# Copyright 2015-2021 Ghent University
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
Tests for the inode_log.py script in vsc-filesystem-quota.

@author: Andy Georges
"""

from vsc.filesystem.gpfs import GpfsQuota
from vsc.filesystem.quota.tools import InodeCritical, process_inodes_information

from vsc.install.testing import TestCase


class TestProcessInodesInformation(TestCase):
    """
    Test the function that processes the information regarding inode usage to determine
    for which filesets the usage has become critical, i.e.m exceeded the given threshold.
    """

    def setUp(self):
        """
        Add example cases.
        """
        super(TestProcessInodesInformation, self).setUp()

        # names reflect the number of used inodes (max is set by default at 100, so this effectively is a percentage.
        self.names = (10, 95)

        self.filesets = {
            self.names[0]: {
                'allocInodes': 90,
                'filesetName': '%d' % self.names[0],
                'maxInodes': 100,
            },
            self.names[1]: {
                'allocInodes': 90,
                'filesetName': '%d' % self.names[1],
                'maxInodes': 100,
            }
        }

        self.defaultQuota = GpfsQuota(
            name="testQuota",
            blockUsage=0,
            blockQuota=0,
            blockLimit=0,
            blockInDoubt=0,
            blockGrace=0,
            filesUsage=0,
            filesQuota=0,
            filesLimit=0,
            filesInDoubt=0,
            filesGrace=0,
            remarks="",
            quota=0,
            defQuota=0,
            fid=0,
            filesetname="default"
        )

        self.usage = {
            self.names[0]: [self.defaultQuota._replace(filesUsage=self.names[0])],
            self.names[1]: [self.defaultQuota._replace(filesUsage=self.names[1])],
        }

    def testThreshold(self):
        """
        Verify that only entries that have a percentage above the threshold are marked as critical.
        """
        critical = process_inodes_information(self.filesets, self.usage, threshold=0.9)

        self.assertEqual(
            critical,
            { str(self.names[1]): InodeCritical(used=self.usage[self.names[1]][0].filesUsage,
                                                allocated=self.filesets[self.names[1]]['allocInodes'],
                                                maxinodes=self.filesets[self.names[1]]['maxInodes']),
            },
            "computed dict with critical filesets is the expected dict"
        )
