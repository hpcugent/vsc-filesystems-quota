#!/usr/bin/env python
#
# Copyright 2021-2023 Ghent University
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
Client-side script to gather quota information stored for the user on lustre and
display it in an understandable format. All quota is on projects.

User quota is the quota on the user fileset/project
By default, all project quota that match the DODRIO_PROJECT_PREFIX will be shown
This can be changed by using the project option

@author: Kenneth Waegeman (Ghent University)
"""
from __future__ import print_function
from vsc.utils.script_tools import SimpleOption
from vsc.filesystem.lustre import LustreOperations

import os
import grp

DODRIO_PROJECT_PREFIX = 'gpr_compute'

def main():

    options = {
        'projects': ("(Only) return quota for these projects (full group names)", "strlist", "store", [], 'p'),
    }
    opts = SimpleOption(options)

    projects = []
    if opts.options.projects:
        names = opts.options.projects
        for grname in names:
            try:
                gid = grp.getgrnam(grname).gr_gid
            except KeyError:
                gid = grp.getgrnam('%s_%s' % (DODRIO_PROJECT_PREFIX, grname)).gr_gid
            projects.append({'name': grname, 'gid' : gid})

    else:
        mygroups = os.getgroups()
        for group in mygroups:
            if DODRIO_PROJECT_PREFIX in grp.getgrgid(group).gr_name:
                projects.append({'name': grp.getgrgid(group).gr_name, 'gid': group})

    myuid = os.getuid()
    lustop = LustreOperations()
    filesystems = lustop.list_filesystems()
    for filesys in filesystems.values():
        path = filesys['defaultMountPoint']
        userquota = lustop.get_project_quota(myuid, path)
        print("Userquota:\n" + userquota)
        for project in projects:
            prjquota = lustop.get_project_quota(project['gid'], path)
            print("Quota for project %s:\n%s" % (project['name'], prjquota))

if __name__ == '__main__':
    main()
