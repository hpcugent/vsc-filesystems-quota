#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2021 Ghent University
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
Client-side script to gather quota information stored for the user on various filesystems and
display it in an understandable format.

Storing the quota information in a cache file accessible only to the user is taking too long
and will take even longer in the future. This approach is no longer used. Users should consult
the account page for cache quota information (with an accuracy of 10 minutes).


@author: Andy Georges (Ghent University)
"""
from __future__ import print_function

def main():

    print("Please consult the VSC account page for quota information at")
    print("https://account.vscentrum.be\n\n")
    print("If you are a VO moderator, you can find the VO quota for all members at")
    print("https://account.vscentrum.be/django/vo/")

if __name__ == '__main__':
    main()
