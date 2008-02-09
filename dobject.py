import dbus
import dbus.service
import dbus.gobject_service
import time
import logging
import threading
import thread

def PassFunction(*args):
    pass

def ReturnFunction(x):
    return x

class TubeBox:
    """ A TubeBox is a box that either contains a Tube or does not.
    The purpose of a TubeBox is to solve this problem: Activities are not
    provided with the sharing Tube until they are shared, but DObjects should
    not have to care whether or not they have been shared.  That means that the
    DObject handler must know whether or not a Tube has been provided.  This
    could be implemented within the handlers, but then the Activity's sharing
    code would have to know a list of all DObject handlers.
    
    Instead, the sharing code just needs to create a TubeBox and pass it to the
    code that creates handlers.  Once the tube arrives, it can be added to the
    TubeBox with insert_tube.  The handlers will then be notified automatically.
    """
    def __init__(self):
        self.tube = None
        self.is_initiator = None
        self._listeners = []
    
    def register_listener(self, L):
        """This method is used by the DObject handlers to add a callback
        function that will be called after insert_tube"""
        self._listeners.append(L)
        if self.tube is not None:
            L(self.tube, self.is_initiator)
    
    def insert_tube(self, tube, is_initiator=False):
        """This method is used by the sharing code to provide the tube, once it
        is ready, along with a boolean indicating whether or not this computer
        is the initiator (who may have special duties, as the first
        participant)."""
        self.tube = tube
        self.is_initiator = is_initiator
        for L in self._listeners:
            L(tube, is_initiator)

class TimeHandler(dbus.gobject_service.ExportedGObject):
    """A TimeHandler provides a universal clock for a sharing instance.  It is a
    sort of cheap, decentralized synchronization system.  The TimeHandler 
    determines the offset between local time and group time by sending a
    broadcast and accepting the first response, and assuming that both transfer
    displays were equal.  The initiator's offset is 0.0, but once another group
    member has synchronized, the initiator can leave and new members will still
    be synchronized correctly.
    
    TimeHandler is not perfectly resilient to disappearances.  If the group
    splits, and one of the daughter groups does not contain any members that
    have had a chance to synchronize, then they will not sync to each other.  I
    am not yet aware of any sensible synchronization system that avoids this 
    problem.
    """
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
        """Callback for the TubeBox"""
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
        """Get the group time"""
        return time.time() + self.offset
        
    def get_offset(self):
        """Get the difference between local time and group time"""
        self._logger.debug("get_offset " + str(self.offset))
        return self.offset
    
    def set_offset(self, offset):
        """Set the difference between local time and group time, and assert that
        this is correct"""
        self._logger.debug("set_offset " + str(offset))
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
                remote.receive_time(asktime, start_time, time.time() + self.offset, reply_handler=PassFunction, error_handler=PassFunction)
        finally:
            return
    
    @dbus.service.method(dbus_interface=IFACE, in_signature='ddd', out_signature='')
    def receive_time(self, asktime, start_time, finish_time):
        self._logger.debug("receive_time")
        rtime = time.time()
        thread.start_new_thread(self._handle_incoming_time, (asktime, start_time, finish_time, rtime))
    
    def _handle_incoming_time(self, ask, start, finish, receive):
        self._offset_lock.acquire()
        if not self._know_offset:
            self.offset = ((start + finish)/2) - ((ask + receive)/2)
            self._know_offset = True
        self._offset_lock.release()


class UnorderedHandler(dbus.gobject_service.ExportedGObject):
    """ The most basic DObject is the Unordered Object (UO).  A UO has the
    property that any changes to its state can be encapsulated as messages, and
    these messages have no intrinsic ordering.  Different instances of the same
    UO, after receiving the same messages in different orders, should reach the
    same state.
    
    Any UO could be implemented as a set of all messages received so far, and
    coherency could be maintained by sending all messages ever transmitted to
    each new joining member.  However, many UOs will have the property that most
    messages are obsolete, and need not be transmitted. Therefore, as an
    optimization, UOs manage their own state structures for synchronizing state
    with joining/merging users.
    
    Each UO should accept a UnorderedHandler as one of its constructor's arguments
    Whenever an action is taken on the local UO (e.g. a method call that changes
    the object's state), the UO must call handler.send() with an appropriately
    encoded message.  Every UO must implement three methods:
    
    receive_message(msg):
    This method accepts and processes a message sent via handler.send().
    Because objects are sent over DBus, it is advisable to DBus-ify the message
    before calling send, and de-DBus-ify it inside receive_message.
    
    get_history():
    This method returns an encoded copy of all non-obsolete state, ready to be
    sent over DBus.
    
    add_history(state):
    This method accepts and processes the state object returned by get_history()
    """
    IFACE = "org.dobject.Unordered"
    BASEPATH = "/org/dobject/Unordered/"

    def __init__(self, name, tube_box):
        """To construct a UO, the program must provide a name and a TubeBox.
        The name is used to identify the UO; all UO with the same name on the
        same Tube should be considered views into the same abstract distributed
        object."""
        self.PATH = UnorderedHandler.BASEPATH + name
        dbus.gobject_service.ExportedGObject.__init__(self)
        self._logger = logging.getLogger(self.PATH)
        self._tube_box = tube_box
        self.tube = None
        
        self.object = None
        self._tube_box.register_listener(self.set_tube)

    def set_tube(self, tube, is_initiator):
        """Callback for the TubeBox"""
        self.tube = tube
        self.add_to_connection(self.tube, self.PATH)
                        
        self.tube.add_signal_receiver(self.receive_message, signal_name='send', dbus_interface=UnorderedHandler.IFACE, sender_keyword='sender', path=self.PATH)
        self.tube.add_signal_receiver(self.tell_history, signal_name='ask_history', dbus_interface=UnorderedHandler.IFACE, sender_keyword='sender', path=self.PATH)
        self.tube.watch_participants(self.members_changed)

        #Alternative implementation of members_changed (not yet working)
        #self.tube.add_signal_receiver(self.members_changed, signal_name="MembersChanged", dbus_interface="org.freedesktop.Telepathy.Channel.Interface.Group")
        
        if self.object is not None:
            self.ask_history()

    def register(self, obj):
        """This method registers obj as the UnorderedObject being managed by
        this Handler.  It is called by obj after obj has initialized itself."""
        self.object = obj
        if self.tube is not None:
            self.ask_history()
            
    def get_path(self):
        """Returns the DBus path of this handler.  The path is the closest thing
        to a unique identifier for each abstract DObject."""
        return self.PATH
    
    def get_tube(self):
        """Returns the TubeBox used to create this handler.  This method is
        necessary if one DObject wishes to create another."""
        return self._tube_box
    
    @dbus.service.signal(dbus_interface=IFACE, signature='v')
    def send(self, message):
        """This method broadcasts message to all other handlers for this UO"""
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
        self._logger.debug("tell_history to " + str(sender))
        try:
            if sender == self.tube.get_unique_name():
                return
            if self.object is None:
                self._logger.error("object not registered before tell_history")
                return
            remote = self.tube.get_object(sender, self.PATH)
            h = self.object.get_history()
            remote.receive_history(h, reply_handler=PassFunction, error_handler=PassFunction)
        finally:
            return
    
    @dbus.service.method(dbus_interface=IFACE, in_signature = 'v', out_signature='')
    def receive_history(self, hist):
        if self.object is None:
            self._logger.error("object not registered before receive_history")
            return
        self.object.add_history(hist)

    #Alternative implementation of a members_changed (not yet working)
    """ 
    def members_changed(self, message, added, removed, local_pending, remote_pending, actor, reason):
        added_names = self.tube.InspectHandles(telepathy.CONNECTION_HANDLE_TYPE_LIST, added)
        for name in added_names:
            self.tell_history(name)
    """
    def members_changed(self, added, removed):
        self._logger.debug("members_changed")
        for (handle, name) in added:
            self.tell_history(sender=name)

def empty_translator(x, pack):
    return x

class HighScore():
    def __init__(self, handler, initval, initscore, value_translator=empty_translator, score_translator=empty_translator):
        self._logger = logging.getLogger('stopwatch.HighScore')
        self._lock = threading.Lock()
        self._value = initval
        self._score = initscore
        
        self._val_trans = value_translator
        self._score_trans = score_translator
        
        self._handler = handler
        self._handler.register(self)
        
        self._listeners = []
    
    def _set_value_from_net(self, val, score):
        self._logger.debug("set_value_from_net " + str(val) + " " + str(score))
        if self._actually_set_value(val, score):
            self._trigger()
    
    def receive_message(self, message):
        self._logger.debug("receive_message " + str(message))
        self._set_value_from_net(self._val_trans(message[0], False), self._score_trans(message[1], False))
    
    add_history = receive_message
    
    def set_value(self, val, score):
        self._logger.debug("set_value " + str(val) + " " + str(score))
        if self._actually_set_value(val, score):
            self._handler.send(self.get_history())
            
    def _actually_set_value(self, value, score):
        self._logger.debug("_actually_set_value " + str(value)+ " " + str(score))
        self._lock.acquire()
        if self._score < score:
            self._value = value
            self._score = score
            self._lock.release()
            return True
        else:
            self._logger.debug("not changing value")
            self._lock.release()
            return False
    
    def get_value(self):
        return self._value
    
    def get_score(self):
        return self._score
    
    def get_pair(self):
        self._lock.acquire()
        pair = (self._value, self._score)
        self._lock.release()
        return pair
    
    def get_history(self):
        p = self.get_pair()
        return (self._val_trans(p[0], True), self._score_trans(p[1], True))
    
    def register_listener(self, L):
        self._lock.acquire()
        self._listeners.append(L)
        self._lock.release()
        (v,s) = self.get_pair()
        L(v,s)
    
    def _trigger(self):
        (v,s) = self.get_pair()
        for L in self._listeners:
            L(v,s)

def float_translator(f, pack):
    if pack:
        return dbus.Double(f)
    else:
        return float(f)

def string_translator(s, pack):
    if pack:
        return dbus.String(s)
    else:
        return str(s)

class Latest:
    def __init__(self, handler, initval, inittime=float('-inf'), time_handler=None, translator=empty_translator):
        if time_handler is None:
            self._time_handler = TimeHandler(handler.get_path(), handler.get_tube())
        else:
            self._time_handler = time_handler
        
        self._listeners = []
        self._lock = threading.Lock()
        
        self._highscore = HighScore(handler, initval, inittime, translator, float_translator)
        self._highscore.register_listener(self._highscore_cb)
    
    def get_value(self):
        return self._highscore.get_value()
    
    def set_value(self, val):
        self._highscore.set_value(val, self._time_handler.time())
    
    def register_listener(self, L):
        self._lock.acquire()
        self._listeners.append(L)
        self._lock.release()
        L(self.get_value())
    
    def _highscore_cb(self, val, score):
        for L in self._listeners:
            L(val)
