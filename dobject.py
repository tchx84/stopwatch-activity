import dbus
import dbus.service
import dbus.gobject_service
import time
import logging
import threading
import thread

class TubeBox:
    """ A TubeBox is a box that either contains a Tube or does not."""
    def __init__(self):
        self.tube = None
        self.is_initiator = None
        self._listeners = []
    
    def register_listener(self, L):
        self._listeners.append(L)
        if self.tube is not None:
            L(self.tube, self.is_initiator)
    
    def insert_tube(self, tube, is_initiator):
        self.tube = tube
        self.is_initiator = is_initiator
        for L in self._listeners:
            L(tube, is_initiator)

class TimeHandler(dbus.gobject_service.ExportedGObject):
    IFACE = "org.dobject.TimeHandler"
    BASEPATH = "/org/dobject/TimeHandler/"

    def __init__(self, name, tube_box, offset=0.0):
        self.PATH = TimeHandler.BASEPATH + name
        dbus.gobject_service.ExportedGObject.__init__(self)
        self._logger = logging.getLogger(self.PATH)
        self._tube_box = tube_box
        self.tube = None
        self.is_initiator = None
        
        self.offset = offset
        self._know_offset = False
        self._offset_lock = threading.Lock()
        
        self._tube_box.register_listener(self.get_tube)
                
    def get_tube(self, tube, is_initiator):
        self._logger.debug("get_tube")
        self._logger.debug(str(is_initiator))
        self.tube = tube
        self.add_to_connection(self.tube, self.PATH)
        self.is_initiator = is_initiator
        self._know_offset = is_initiator
        self.tube.add_signal_receiver(self.tell_time, signal_name='What_time_is_it', dbus_interface=TimeHandler.IFACE, sender_keyword='sender', path=self.PATH)

        if not self._know_offset:
            self.ask_time()

    def time(self):
        return time.time() + self.offset
        
    def get_offset(self):
        self._logger.debug("offset= " + str(self.offset))
        return self.offset
    
    def set_offset(self, offset):
        self._logger.debug("set_offset " + str(self.offset))
        self._offset_lock.acquire()
        self.offset = offset
        self._know_offset = True
        self._offset_lock.release()

    @dbus.service.signal(dbus_interface=IFACE, signature='d')
    def What_time_is_it(self, asktime):
        return
        
    def ask_time(self):
        self._logger.debug("ask_time")
        self.What_time_is_it(time.time())
    
    def tell_time(self, asktime, sender=None):
        self._logger.debug("tell_time")
        start_time = time.time()
        try:
            my_name = self.tube.get_unique_name()
            if sender == my_name:
                return
            if self._know_offset:
                self._logger.debug("telling offset")
                remote = self.tube.get_object(sender, self.PATH)
                start_time += self.offset
                remote.receive_time(asktime, start_time, time.time() + self.offset)
        finally:
            return
    
    @dbus.service.method(dbus_interface=IFACE, in_signature='ddd', out_signature='')
    def receive_time(self, asktime, start_time, finish_time):
        self._logger.debug("receive_time")
        rtime = time.time()
        thread.start_new_thread(self._handle_incoming_time, (asktime, start_time, finish_time, rtime))
    
    def _handle_incoming_time(self, ask, start, finish, receive):
        print(self.offset)
        self._offset_lock.acquire()
        if not self._know_offset:
            self.offset = ((start + finish)/2) - ((ask + receive)/2)
            self._know_offset = True
        self._offset_lock.release()
        print(self.offset)


class UnorderedHandler(dbus.gobject_service.ExportedGObject):
    IFACE = "org.dobject.Unordered"
    BASEPATH = "/org/dobject/Unordered/"

    def __init__(self, name, tube_box):
        self.PATH = UnorderedHandler.BASEPATH + name
        dbus.gobject_service.ExportedGObject.__init__(self)
        self._logger = logging.getLogger(self.PATH)
        self._tube_box = tube_box
        self.tube = None
        
        self.object = None
        self._tube_box.register_listener(self.get_tube)

    def get_tube(self, tube, is_initiator):
        self.tube = tube
        self.add_to_connection(self.tube, self.PATH)
                        
        self.tube.add_signal_receiver(self.receive_message, signal_name='send', dbus_interface=UnorderedHandler.IFACE, sender_keyword='sender', path=self.PATH)
        self.tube.add_signal_receiver(self.tell_history, signal_name='ask_history', dbus_interface=UnorderedHandler.IFACE, sender_keyword='sender', path=self.PATH)
        self.tube.add_signal_receiver(self.members_changed, signal_name="MembersChanged", dbus_interface="org.freedesktop.Telepathy.Channel.Interface.Group")
        
        if self.object is not None:
            self.ask_history()

    def register(self, obj):
        self.object = obj
        if self.tube is not None:
            self.ask_history()
    
    @dbus.service.signal(dbus_interface=IFACE, signature='v')
    def send(self, message):
        return
        
    def receive_message(self, message, sender=None):
        if self.object is None:
            self._logger.error("got message before registration")
        else:
            self.object.receive_message(message)
    
    @dbus.service.signal(dbus_interface=IFACE, signature='')
    def ask_history(self):
        return
    
    def tell_history(self, sender=None):
        try:
            if sender == self.tube.get_unique_name():
                return
            if self.object is None:
                self._logger.error("object not registered before tell_history")
                return
            remote = self.tube.get_object(sender, self.PATH)
            h = self.object.get_history()
            remote.receive_history(h)
        finally:
            return
    
    @dbus.service.method(dbus_interface=IFACE, in_signature = 'v', out_signature='')
    def receive_history(self, hist):
        if self.object is None:
            self._logger.error("object not registered before receive_history")
            return
        self.object.add_history(hist)

    def members_changed(self, message, added, removed, local_pending, remote_pending, actor, reason):
        added_names = self.tube.InspectHandles(telepathy.CONNECTION_HANDLE_TYPE_LIST, added)
        for name in added_names:
            self.tell_history(name)
            
class UserDict(dbus.gobject_service.ExportedGObject):
    IFACE = "org.dobject.UserDict"
    BASEPATH = "/org/dobject/UserDict/"
    
    _thedict = {}

    def __init__(self, name, tube_conn):
        self.PATH = UserDict.BASEPATH + name
        dbus.gobject_service.ExportedGObject.__init__(self, tube_conn, self.PATH)
        self._logger = logging.getLogger(self.PATH)
        self.tube = tube_conn

        self.tube.add_signal_receiver(self.receive_value, signal_name='set_my_value', dbus_interface=UserDict.IFACE, sender_keyword='sender', path=self.PATH)
        self.tube.add_signal_receiver(self.members_changed, signal_name="MembersChanged", dbus_interface="org.freedesktop.Telepathy.Channel.Interface.Group")
        
    @dbus.service.signal(dbus_interface=IFACE)
    def set_my_value(self, val):
        self._thedict[self.tube.get_unique_name()] = val
        return
        
    def receive_value(self, val, sender=None):
        if self.object is None:
            self._logger.error("got message without sender")
        else:
            self._thedict[sender] = val
