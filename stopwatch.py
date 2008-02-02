# Copyright 2007 Benjamin M. Schwartz
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

import gtk
import gtk.gdk
import gobject
import dbus
import dbus.service
import dbus.gobject_service
import logging
import time
import thread
import threading
import cPickle
import sets
import bisect
import locale
import pango
from gettext import gettext

IFACE = "org.laptop.StopWatch"
PATH = "/org/laptop/StopWatch"

class TubeHandler(dbus.gobject_service.ExportedGObject):
    def __init__(self, tube_conn, initiating, model, controller):
        dbus.gobject_service.ExportedGObject.__init__(self, tube_conn, PATH)
        self._logger = logging.getLogger('stopwatch.TubeHandler')
        self.tube = tube_conn
        self.is_initiator = initiating
        self.model = model
        self.controller = controller
        
        self._know_offset = self.is_initiator
        self._offset_lock = threading.Lock()
        self._history_lock = threading.Lock()
                
        self.tube.add_signal_receiver(self.tell_time, signal_name='What_time_is_it', dbus_interface=IFACE, sender_keyword='sender')
        self.tube.add_signal_receiver(self.tell_history, signal_name='What_has_happened', dbus_interface=IFACE, sender_keyword='sender')
        self.tube.add_signal_receiver(self.tell_names, signal_name='What_are_the_names', dbus_interface=IFACE, sender_keyword='sender')
        self.tube.add_signal_receiver(self.receive_history, signal_name='event_broadcast', dbus_interface=IFACE, byte_arrays=True)
        self.tube.add_signal_receiver(self.receive_name, signal_name='name_change_broadcast', dbus_interface=IFACE, sender_keyword='sender', utf8_strings=True)
        
        self.controller.register_time_listener(self.event_listener)
        self.controller.register_name_listener(self.name_listener)
        
        if not self._know_offset:
            self.ask_time()
        
        if not self.is_initiator:
            self.ask_history()
            self.ask_names()

    @dbus.service.signal(dbus_interface=IFACE, signature='d')
    def What_time_is_it(self, asktime):
        return
        
    def ask_time(self):
        self.What_time_is_it(time.time())
    
    def tell_time(self, asktime, sender=None):
        start_time = time.time()
        if sender == self.tube.get_unique_name():
            return
        if self._know_offset:
            remote = self.tube.get_object(sender, PATH)
            offset = self.model.get_offset()
            start_time += offset
            remote.receive_time(asktime, start_time, time.time() + offset)
    
    @dbus.service.method(dbus_interface=IFACE, in_signature='ddd', out_signature='')
    def receive_time(self, asktime, start_time, finish_time):
        rtime = time.time()
        thread.start_new_thread(self._handle_incoming_time, (asktime, start_time, finish_time, rtime))
    
    def _handle_incoming_time(self, ask, start, finish, receive):
        self._offset_lock.acquire()
        if not self._know_offset:
            offset = ((start + finish)/2) - ((ask + receive)/2)
            self.model.set_offset(offset)
            self._know_offset = True
        self._offset_lock.release()

    @dbus.service.signal(dbus_interface=IFACE, signature='')
    def What_has_happened(self):
        return
    
    def ask_history(self):
        self.What_has_happened()
    
    def tell_history(self, sender=None):
        if sender == self.tube.get_unique_name():
            return
        remote = self.tube.get_object(sender, PATH)
        h = self.model.get_history()
        remote.receive_history(cPickle.dumps(h))
    
    @dbus.service.method(dbus_interface=IFACE, in_signature='ay', out_signature='', byte_arrays=True, utf8_strings=True)
    def receive_history(self, hist_string):
        thread.start_new_thread(self._handle_incoming_history, (hist_string,))
    
    def _handle_incoming_history(self, hist_string):
        self._history_lock.acquire()
        h = cPickle.loads(hist_string)
        self.model.add_history(h)
        self._history_lock.release()

    @dbus.service.signal(dbus_interface=IFACE, signature='')
    def What_are_the_names(self):
        return

    def ask_names(self):
        self._logger.debug("ask_names")
        self.What_are_the_names()
    
    def tell_names(self, sender=None):
        self._logger.debug("tell_names")
        if sender == self.tube.get_unique_name():
            return
        remote = self.tube.get_object(sender, PATH)
        n = self.model.get_all_names()
        remote.receive_all_names(n)
    
    @dbus.service.method(dbus_interface=IFACE, in_signature='(asad)', out_signature='', utf8_strings=True)
    def receive_all_names(self, names_and_times):
        self._logger.debug("receive_names")
        thread.start_new_thread(self._handle_incoming_names, (names_and_times,))
    
    def _handle_incoming_names(self, names):
        self._logger.debug("_handle_incoming_names")
        self.model.set_all_names(names)
    
    @dbus.service.signal(dbus_interface=IFACE, signature='ay')
    def event_broadcast(self, hist_string):
        return
    
    def event_listener(self, h):
        self.event_broadcast(cPickle.dumps(h))
    
    @dbus.service.signal(dbus_interface=IFACE, signature='isd')
    def name_change_broadcast(self, i, name, t):
        self._logger.debug("name_change_broadcast")
        return
    
    def name_listener(self, i, name, t):
        self._logger.debug("name_listener")
        self.name_change_broadcast(i, name, t)
        return
        
    def receive_name(self, i, name, t, sender=None):
        self._logger.debug("receive_name " + name)
        if sender != self.tube.get_unique_name():
            self.model.set_name(i, name, t)
    
class WatchEvent():
    RUN_EVENT = 1
    PAUSE_EVENT = 2
    RESET_EVENT = 3
    def __init__(self, event_time, event_type, watch_id):
        self._event_time = event_time
        self._event_type = event_type
        self._watch_id = watch_id
    
    def get_time(self):
        return self._event_time
    
    def get_type(self):
        return self._event_type
    
    def get_watch(self):
        return self._watch_id
        
    def _tuple(self):
        return (self._event_time, self._event_type, self._watch_id)
        
    def __cmp__(self, other):
        return cmp(self._tuple(), other)
    
    def __hash__(self):
        return hash(self._tuple())
    

class Model():
    NUM_WATCHES = 10
    
    STATE_PAUSED = 1
    STATE_RUNNING = 2

    def __init__(self):
        self._logger = logging.getLogger('stopwatch.Model')
        self._known_events = sets.Set()
        self._history = [[] for i in xrange(Model.NUM_WATCHES)]
        self._history_lock = threading.RLock()

        self._offset = 0.0
        
        self._init_state = [(Model.STATE_PAUSED, 0) for i in xrange(Model.NUM_WATCHES)]
        self._state = [(Model.STATE_PAUSED, 0) for i in xrange(Model.NUM_WATCHES)]
        self._names = [gettext("Stopwatch") + " " + locale.str(i+1) for i in xrange(Model.NUM_WATCHES)]
        self._name_times = [float('-inf')] * Model.NUM_WATCHES
        self._name_lock = threading.RLock()
        
        self._time_listeners = [[] for i in xrange(Model.NUM_WATCHES)]
        self._name_listeners = [[] for i in xrange(Model.NUM_WATCHES)]
        
    def get_all(self):
        return (self.get_all_names(), self._state, self._offset)
        
    def reset(self, trio):
        self._history_lock.acquire()
        self.set_all_names(trio[0])
        self._init_state = trio[1]
        self._offset = trio[2]
        self._history = [[] for i in xrange(Model.NUM_WATCHES)]
        self._state = [() for i in xrange(Model.NUM_WATCHES)]
        for i in xrange(Model.NUM_WATCHES):
            self._update_state(i)
        self._history_lock.release()
    
    def get_offset(self):
        return self._offset
    
    def set_offset(self, x):
        self._offset = x
        for i in xrange(Model.NUM_WATCHES):
            self._trigger(i)
    
    def get_history(self):
        return self._history
    
    def get_name(self, i):
        return self._names[i]
    
    def set_name(self, i, name, t):
        self._logger.debug("set_name" + str(i) + " " + name)
        if self.set_name_silent(i, name, t):
            self._name_trigger(i)
            
    def set_name_silent(self, i, name, t):
        self._logger.debug("set_name_silent" + str(i) + " " + name)
        self._name_lock.acquire()
        if self._name_times[i] <= t:
            self._names[i] = str(name)
            self._name_times[i] = float(t)
            self._name_lock.release()
            return True
        else:
            self._name_lock.release()
            return False
    
    def get_all_names(self):
        return (self._names, self._name_times)
    
    def set_all_names(self, n):
        for i in xrange(Model.NUM_WATCHES):
            self.set_name(i, n[0][i], n[1][i])
    
    def add_history(self, h):
        self._logger.debug("add_history")
        assert len(h) == Model.NUM_WATCHES
        self._history_lock.acquire()
        for i in xrange(Model.NUM_WATCHES):
            w = h[i]
            changed = False
            for ev in w:
                if ev not in self._known_events:
                    self._known_events.add(ev)
                    bisect.insort(self._history[i], ev)
                    changed = True
            if changed:
                self._update_state(i)
        self._history_lock.release()
        
    def _update_state(self, i):
        self._logger.debug("_update_state")
        w = self._history[i]
        L = len(w)
        s = self._init_state[i][0]
        timeval = self._init_state[i][1]
        #state machine
        for ev in w:
            event_type = ev.get_type()
            if s == Model.STATE_PAUSED:
                if event_type == WatchEvent.RUN_EVENT:
                    s = Model.STATE_RUNNING
                    timeval = ev.get_time() - timeval
                elif event_type == WatchEvent.RESET_EVENT:
                    timeval = 0
            elif s == Model.STATE_RUNNING:
                if event_type == WatchEvent.RESET_EVENT:
                    timeval = ev.get_time()
                elif event_type == WatchEvent.PAUSE_EVENT:
                    s = Model.STATE_PAUSED
                    timeval = ev.get_time() - timeval

        self._set_state(i, (s, timeval))
        
    def _set_state(self, i, q):
        self._logger.debug("_set_state")
        if self._state[i] != q:
            self._state[i] = q
            self._trigger(i)
    
    def _name_trigger(self, i):
        self._logger.debug("_name_trigger")
        for l in self._name_listeners[i]:
            thread.start_new_thread(l, (self._names[i],))
    
    def _trigger(self, i):
        self._logger.debug("_trigger")
        for l in self._time_listeners[i]:
            thread.start_new_thread(l, (self._state[i],))    
    
    def register_time_listener(self, i, l):
        self._logger.debug("register_time_listener " + str(i) + " " + str(l))
        self._time_listeners[i].append(l)
        self._logger.debug(str(self._time_listeners))
    
    def register_name_listener(self, i, l):
        self._logger.debug("register_name_listener " + str(i) + " " + str(l))
        self._name_listeners[i].append(l)
        self._logger.debug(str(self._name_listeners))

class Controller():
    def __init__(self, model):
        self._logger = logging.getLogger('stopwatch.Controller')
        self._model = model
        self._time_listeners = [self._model.add_history]
        self._name_listeners = [self._model.set_name_silent]
    
    def register_time_listener(self, l):
        self._time_listeners.append(l)
    
    def register_name_listener(self, l):
        self._name_listeners.append(l)
        
    def _trigger(self, h):
        self._logger.debug("_trigger")
        for l in self._time_listeners:
            thread.start_new_thread(l, (h,))
    
    def _do_event(self, i, time, event_type):
        ev = WatchEvent(time, event_type, i)
        h = [[] for k in xrange(Model.NUM_WATCHES)]
        h[i].append(ev)
        self._trigger(h)
    
    def run(self, i, time):
        self._logger.debug("run "+ str(time))
        self._do_event(i, time, WatchEvent.RUN_EVENT)
    
    def pause(self, i, time):
        self._logger.debug("pause "+ str(time))
        self._do_event(i, time, WatchEvent.PAUSE_EVENT)
    
    def reset(self, i, time):
        self._logger.debug("reset "+ str(time))
        self._do_event(i, time, WatchEvent.RESET_EVENT)
    
    def set_name(self, i, name, t):
        self._logger.debug("set_name "+ name)
        for f in self._name_listeners:
            thread.start_new_thread(f, (i, name, t))

class OneWatchView():
    def __init__(self, watch_id, model, controller):
        self._logger = logging.getLogger('stopwatch.OneWatchView'+str(watch_id))
        self._watch_id = watch_id
        self._model = model
        self._controller = controller
        
        self._state = Model.STATE_PAUSED
        self._timeval = 0
        
        self._model.register_time_listener(self._watch_id, self.update_state)
        self._offset = self._model.get_offset()
        
        self._name = gtk.Entry()
        self._name.set_text(self._model.get_name(self._watch_id))
        self._name_changed_handler = self._name.connect('changed', self._name_cb)
        self._model.register_name_listener(self._watch_id, self.update_name)
        self._name_lock = threading.Lock()
        
        check = gtk.Image()
        check.set_from_file('check.svg')
        self._run_button = gtk.ToggleButton(gettext("Start/Stop"))
        self._run_button.set_image(check)
        self._run_button.props.focus_on_click = False        
        self._run_handler = self._run_button.connect('clicked', self._run_cb)
        self._run_button_lock = threading.Lock()

        circle = gtk.Image()
        circle.set_from_file('circle.svg')
        self._reset_button = gtk.Button(gettext("Zero"))
        self._reset_button.set_image(circle)
        self._reset_button.props.focus_on_click = False
        self._reset_button.connect('clicked', self._reset_cb)
        
        timefont = pango.FontDescription()
        timefont.set_family("monospace")
        timefont.set_size(pango.SCALE*14)
        self._time_label = gtk.Label(self._format(0))
        self._time_label.modify_font(timefont)
        self._time_label.set_single_line_mode(True)
        self._time_label.set_selectable(True)
        self._time_label.set_width_chars(10)
        self._time_label.set_alignment(1,0.5) #justify right
        self._time_label.set_padding(6,0)
        eb = gtk.EventBox()
        eb.add(self._time_label)
        eb.modify_bg(gtk.STATE_NORMAL, gtk.gdk.color_parse("white"))
        
        self._should_update = threading.Event()
        self._is_visible = threading.Event()
        self._is_visible.set()
        self._update_lock = threading.Lock()
        self._label_lock = threading.Lock()

        self.box = gtk.HBox()
        self.box.pack_start(self._name, padding=6)
        self.box.pack_start(self._run_button, expand=False)
        self.box.pack_start(self._reset_button, expand=False)
        self.box.pack_end(eb, expand=False, padding=6)
        
        filler = gtk.VBox()
        filler.pack_start(self.box, expand=True, fill=False)
        
        self.backbox = gtk.EventBox()
        self.backbox.add(filler)
        self._black = gtk.gdk.color_parse("black")
        self._gray = gtk.gdk.Color(256*192, 256*192, 256*192)
        
        self.display = gtk.EventBox()
        self.display.add(self.backbox)
        #self.display.set_above_child(True)
        self.display.props.can_focus = True
        self.display.connect('focus-in-event', self._got_focus_cb)
        self.display.connect('focus-out-event', self._lost_focus_cb)
        self.display.add_events(gtk.gdk.ALL_EVENTS_MASK)
        self.display.connect('key-press-event', self._keypress_cb)
        #self.display.connect('key-release-event', self._keyrelease_cb)
        
        thread.start_new_thread(self._start_running, ())
        
    def update_state(self, q):
        self._logger.debug("update_state: "+str(q))
        self._update_lock.acquire()
        self._logger.debug("acquired update_lock")
        self._state = q[0]
        self._offset = self._model.get_offset()
        if self._state == Model.STATE_RUNNING:
            self._timeval = q[1]
            self._set_run_button_active(True)
            self._should_update.set()
        else:
            self._set_run_button_active(False)
            self._should_update.clear()
            self._label_lock.acquire()
            self._timeval = q[1]
            ev = threading.Event()
            gobject.idle_add(self._update_label, self._format(self._timeval), ev)
            ev.wait()
            self._label_lock.release()
        self._update_lock.release()
    
    def update_name(self, name):
        self._name_lock.acquire()
        self._name.set_editable(False)
        self._name.handler_block(self._name_changed_handler)
        ev = threading.Event()
        gobject.idle_add(self._set_name, name, ev)
        ev.wait()
        self._name.handler_unblock(self._name_changed_handler)
        self._name.set_editable(True)
        self._name_lock.release()
            
    def _set_name(self, name, ev):
        self._name.set_text(name)
        ev.set()
        return False
        
    def _format(self, t):
        return locale.format('%.2f',t)
    
    def _update_label(self, string, ev):
        self._time_label.set_text(string)
        ev.set()
        return False
    
    def _start_running(self):
        self._logger.debug("_start_running")
        ev = threading.Event()
        while True:
            self._should_update.wait()
            self._is_visible.wait()
            self._label_lock.acquire()
            if self._should_update.isSet() and self._is_visible.isSet():
                s = self._format(time.time() + self._offset - self._timeval)
                ev.clear()
                gobject.idle_add(self._update_label, s, ev)
                ev.wait()
                time.sleep(0.07)
            self._label_lock.release()
    
    def _run_cb(self, widget):
        t = time.time()
        self._logger.debug("run button pressed: " + str(t))
        if self._run_button.get_active(): #button has _just_ been set active
            self._controller.run(self._watch_id, self._offset + t)
        else:
            self._controller.pause(self._watch_id, self._offset + t)
        return True
        
    def _set_run_button_active(self, v):
        self._run_button_lock.acquire()
        self._run_button.handler_block(self._run_handler)
        self._run_button.set_active(v)
        self._run_button.handler_unblock(self._run_handler)
        self._run_button_lock.release()
            
    def _reset_cb(self, widget):
        t = time.time()
        self._logger.debug("reset button pressed: " + str(t))
        self._controller.reset(self._watch_id, self._offset + t)
        return True
    
    def _name_cb(self, widget):
        t = time.time()
        self._controller.set_name(self._watch_id, widget.get_text(), self._offset + t)
        return True
        
    def pause(self):
        self._logger.debug("pause")
        self._is_visible.clear()
    
    def resume(self):
        self._logger.debug("resume")
        self._is_visible.set()
    
    def _got_focus_cb(self, widget, event):
        self._logger.debug("got focus")
        self.backbox.modify_bg(gtk.STATE_NORMAL, self._black)
        self._name.modify_bg(gtk.STATE_NORMAL, self._black)
        return True
    
    def _lost_focus_cb(self, widget, event):
        self._logger.debug("lost focus")
        self.backbox.modify_bg(gtk.STATE_NORMAL, self._gray)
        self._name.modify_bg(gtk.STATE_NORMAL, self._gray)
        return True
    
    
    # KP_End == check gamekey = 65436
    # KP_Page_Down == X gamekey = 65435
    # KP_Home == box gamekey = 65429
    # KP_Page_Up == O gamekey = 65434
    def _keypress_cb(self, widget, event):
        self._logger.debug("key press: " + gtk.gdk.keyval_name(event.keyval)+ " " + str(event.keyval))
        if event.keyval == 65436:
            self._run_button.clicked()
        elif event.keyval == 65434:
            self._reset_button.clicked()
        return False
            
class GUIView():
    def __init__(self, model, controller):
        self._watches = [OneWatchView(i, model, controller) for i in xrange(Model.NUM_WATCHES)]
        self.display = gtk.VBox()
        for x in self._watches:
            self.display.pack_start(x.display, expand=True, fill=True)
        
        self._pause_lock = threading.Lock()
    
    def pause(self):
        self._pause_lock.acquire()
        for w in self._watches:
            w.pause()
        self._pause_lock.release()
    
    def resume(self):
        self._pause_lock.acquire()
        for w in self._watches:
            w.resume()
        self._pause_lock.release()
    
        
