# Copyright 2007 Collabora Ltd.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

"""HelloMesh Activity: A case study for collaboration using Tubes."""

import logging
import telepathy

from sugar.activity.activity import Activity, ActivityToolbox
from sugar.presence import presenceservice

from sugar.presence.tubeconn import TubeConnection

import stopwatch
import gobject
import dobject

import cPickle
import gtk.gdk

SERVICE = "org.laptop.StopWatch"

class StopWatchActivity(Activity):
    """StopWatch Activity as specified in activity.info"""
    def __init__(self, handle):
        """Set up the StopWatch activity."""
        Activity.__init__(self, handle)
        self._logger = logging.getLogger('stopwatch-activity')
        
        gobject.threads_init()

        # top toolbar with share and close buttons:
        toolbox = ActivityToolbox(self)
        self.set_toolbox(toolbox)
        toolbox.show()

        self.tubebox = dobject.TubeBox()
        self.timer = dobject.TimeHandler("main", self.tubebox)
        self.gui = stopwatch.GUIView(self.tubebox, self.timer)
        
        self.set_canvas(self.gui.display)
        self.show_all()

        self.initiating = False

        # get the Presence Service
        self.pservice = presenceservice.get_instance()
        # Buddy object for you
        owner = self.pservice.get_owner()
        self.owner = owner

        self.connect('shared', self._shared_cb)
        self.connect('joined', self._joined_cb)
        
        self.add_events(gtk.gdk.VISIBILITY_NOTIFY_MASK)
        self.connect("visibility-notify-event", self._visible_cb)
        self.connect("notify::active", self._active_cb)


    def _shared_cb(self, activity):
        self._logger.debug('My activity was shared')
        self.initiating = True
        self._sharing_setup()

        self._logger.debug('This is my activity: making a tube...')
        id = self.tubes_chan[telepathy.CHANNEL_TYPE_TUBES].OfferDBusTube(
            SERVICE, {})

    def _sharing_setup(self):
        if self._shared_activity is None:
            self._logger.error('Failed to share or join activity')
            return

        self.conn = self._shared_activity.telepathy_conn
        self.tubes_chan = self._shared_activity.telepathy_tubes_chan
        self.text_chan = self._shared_activity.telepathy_text_chan

        self.tubes_chan[telepathy.CHANNEL_TYPE_TUBES].connect_to_signal('NewTube',
            self._new_tube_cb)

    def _list_tubes_reply_cb(self, tubes):
        for tube_info in tubes:
            self._new_tube_cb(*tube_info)

    def _list_tubes_error_cb(self, e):
        self._logger.error('ListTubes() failed: %s', e)

    def _joined_cb(self, activity):
        if not self._shared_activity:
            return

        self._logger.debug('Joined an existing shared activity')
        self.initiating = False
        self._sharing_setup()

        self._logger.debug('This is not my activity: waiting for a tube...')
        self.tubes_chan[telepathy.CHANNEL_TYPE_TUBES].ListTubes(
            reply_handler=self._list_tubes_reply_cb,
            error_handler=self._list_tubes_error_cb)

    def _new_tube_cb(self, id, initiator, type, service, params, state):
        self._logger.debug('New tube: ID=%d initator=%d type=%d service=%s '
                     'params=%r state=%d', id, initiator, type, service,
                     params, state)
        if (type == telepathy.TUBE_TYPE_DBUS and
            service == SERVICE):
            if state == telepathy.TUBE_STATE_LOCAL_PENDING:
                self.tubes_chan[telepathy.CHANNEL_TYPE_TUBES].AcceptDBusTube(id)
            tube_conn = TubeConnection(self.conn,
                self.tubes_chan[telepathy.CHANNEL_TYPE_TUBES],
                id, group_iface=self.text_chan[telepathy.CHANNEL_INTERFACE_GROUP])
            self.tubebox.insert_tube(tube_conn, self.initiating)
    
    def read_file(self, file_path):
        f = open(file_path, 'r')
        q = cPickle.load(f)
        f.close()
        self.gui.set_all(q)
    
    def write_file(self, file_path):
        q = self.gui.get_all()
        f = open(file_path, 'w')
        cPickle.dump(q, f)
        f.close()
        
    def _active_cb(self, widget, event):
        self._logger.debug("_active_cb")
        if self.props.active:
            self.gui.resume()
        else:
            self.gui.pause()
            
    def _visible_cb(self, widget, event):
        self._logger.debug("_visible_cb")
        if event.state == gtk.gdk.VISIBILITY_FULLY_OBSCURED:
            self.gui.pause()
        else:
            self.gui.resume()
