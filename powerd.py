# Copyright (C) 2011 One Laptop per Child
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty ofwa
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

import os


def marker():
    """ filesystem path of per-process inhibit file """
    return os.path.join('/var/run/powerd-inhibit-suspend', str(os.getpid()))


class Suspend():
    """ control of powerd idle suspend,
        reference counted,
        does nothing if powerd is not present """

    def __init__(self):
        self.references = 0

    def inhibit(self):
        """ inhibit suspend for this process """
        if self.references == 0:
            try:
                file(marker(), 'w').write('')
            except:
                return
        self.references += 1

    def uninhibit(self):
        """ uninhibit suspend for this process """
        self.references -= 1
        if self.references > 0:
            return
        try:
            os.remove(marker())
        except:
            pass
        self.references = 0
