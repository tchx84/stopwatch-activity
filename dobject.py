"""
Copyright 2008 Benjamin M. Schwartz

DOBject is LGPLv2+

DObject is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 2 of the License, or
(at your option) any later version.

DObject is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with DObject.  If not, see <http://www.gnu.org/licenses/>.
"""
import dbus
import dbus.service
import dbus.gobject_service
import time
import logging
import threading
import thread
import random
from dobject_helpers import *

"""
DObject is a library of components useful for constructing distributed
applications that need to maintain coherent state while communicating over
Telepathy.  The DObject tools are design to handle unexpected joins, leaves,
splits, and merges automatically, and always to leave each connected component
of users in a coherent state at quiescence.
"""

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
    be synchronized correctly.  Errors at each synchronization are typically
    between 0.1s and 2s.
    
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
        self._myname = name
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
    
    def __repr__(self):
        return 'UnorderedHandler(' + self._myname + ', ' + repr(self._tube_box) + ')'
    
    def copy(self, name):
        """A convenience function for returning a new UnorderedHandler derived
        from this one, with a new name.  This is safe as long as copy() is called
        with a different name every time."""
        return UnorderedHandler(self._myname + "/" + name, self._tube_box)

def empty_translator(x, pack):
    return x

class HighScore:
    """ A HighScore is the simplest nontrivial DObject.  A HighScore's state consists
    of a value and a score.  The user may suggest a new value and score.  If the new
    score is higher than the current score, then the value and score are updated.
    Otherwise, they are not.
    
    The value can be any object, and the score can be any comparable object.
    
    To ensure that serialization works correctly, the user may specify a
    translator function that converts values or scores to and from a format that
    can be serialized reliably by dbus-python.
    
    In the event of a tie, coherence cannot be guaranteed.  If ties are likely
    with the score of choice, the user may set break_ties=True, which appends a
    random number to each message, and thereby reduces the probability of a tie
    by a factor of 2**52.
    """
    def __init__(self, handler, initval, initscore, value_translator=empty_translator, score_translator=empty_translator, break_ties=False):
        self._logger = logging.getLogger('stopwatch.HighScore')
        self._lock = threading.Lock()
        self._value = initval
        self._score = initscore
        
        self._break_ties = break_ties
        if self._break_ties:
            self._tiebreaker = random.random()
        else:
            self._tiebreaker = None
        
        self._val_trans = value_translator
        self._score_trans = score_translator
        
        self._handler = handler
        self._handler.register(self)
        
        self._listeners = []
    
    def _set_value_from_net(self, val, score, tiebreaker):
        self._logger.debug("set_value_from_net " + str(val) + " " + str(score))
        if self._actually_set_value(val, score, tiebreaker):
            self._trigger()
    
    def receive_message(self, message):
        self._logger.debug("receive_message " + str(message))
        if len(message) == 2: #Remote has break_ties=False
            self._set_value_from_net(self._val_trans(message[0], False), self._score_trans(message[1], False), None)
        elif len(message) == 3:
            self._set_value_from_net(self._val_trans(message[0], False), self._score_trans(message[1], False), float_translator(message[2], False))
            
    
    add_history = receive_message
    
    def set_value(self, val, score):
        """This method suggests a value and score for this HighScore.  If the
        suggested score is higher than the current score, then both value and
        score will be broadcast to all other participants.
        """
        self._logger.debug("set_value " + str(val) + " " + str(score))
        if self._actually_set_value(val, score, None):
            self._handler.send(self.get_history())
            
    def _actually_set_value(self, value, score, tiebreaker):
        self._logger.debug("_actually_set_value " + str(value)+ " " + str(score))
        if self._break_ties and (tiebreaker is None):
            tiebreaker = random.random()
        self._lock.acquire()
        if self._break_ties: 
            if (self._score < score) or ((self._score == score) and (self._tiebreaker < tiebreaker)):
                self._value = value
                self._score = score
                self._tiebreaker = tiebreaker
                self._lock.release()
                return True
            else:
                self._lock.release()
                return False
        elif self._score < score:
            self._value = value
            self._score = score
            self._lock.release()
            return True
        else:
            self._logger.debug("not changing value")
            self._lock.release()
            return False
    
    def get_value(self):
        """ Get the current winning value."""
        return self._value
    
    def get_score(self):
        """ Get the current winning score."""
        return self._score
    
    def get_pair(self):
        """ Get the current value and score, returned as a tuple (value, score)"""
        self._lock.acquire()
        pair = (self._value, self._score)
        self._lock.release()
        return pair
    
    def _get_all(self):
        if self._break_ties:
            self._lock.acquire()
            q = (self._value, self._score, self._tiebreaker)
            self._lock.release()
            return q
        else:
            return self.get_pair()
    
    def get_history(self):
        p = self._get_all()
        if self._break_ties:
            return (self._val_trans(p[0], True), self._score_trans(p[1], True), float_translator(p[2], True))
        else:
            return (self._val_trans(p[0], True), self._score_trans(p[1], True))
    
    def register_listener(self, L):
        """Register a function L that will be called whenever another user sets
        a new record.  L must have the form L(value, score)."""
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
    """This translator packs and unpacks floats for dbus serialization"""
    if pack:
        return dbus.Double(f)
    else:
        return float(f)

def string_translator(s, pack):
    """This translator packs and unpacks unicode strings for dbus serialization"""
    if pack:
        return dbus.String(s)
    else:
        return str(s)

class Latest:
    """ Latest is a variation on HighScore, in which the score is the current
    timestamp.  Latest uses TimeHandler to provide a groupwide coherent clock.
    Because TimeHandler's guarantees about synchronization and resilience are
    weak, Latest is not as resilient to failures as a true DObject.
    
    The creator must provide a UnorderedHandler and the initial value.  One may
    optionally indicate the initial time (as a float in epoch-time), a
    TimeHandler (otherwise a new one will be created), and a translator for
    serialization of the values.
    """
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
        """ Returns the latest value """
        return self._highscore.get_value()
    
    def set_value(self, val):
        """ Suggest a new value """
        self._highscore.set_value(val, self._time_handler.time())
    
    def register_listener(self, L):
        """ Register a listener L(value), to be called whenever another user
        adds a new latest value."""
        self._lock.acquire()
        self._listeners.append(L)
        self._lock.release()
        L(self.get_value())
    
    def _highscore_cb(self, val, score):
        for L in self._listeners:
            L(val)

class AddOnlySet:
    """The AddOnlySet is the archetypal UnorderedObject.  It consists of a set,
    supporting all the normal Python set operations except those that cause an
    item to be removed from the set.  Thanks to this restriction, a AddOnlySet
    is perfectly coherent, since the order in which elements are added is not
    important.
    """
    def __init__(self, handler, initset = (), translator=empty_translator):
        self._logger = logging.getLogger('dobject.AddOnlySet')
        self._set = set(initset)
        
        self._lock = threading.Lock()

        self._trans = translator
        self._listeners = []  #This must be done before registering with the handler

        self._handler = handler
        self._handler.register(self)
        
        self.__and__ = self._set.__and__
        self.__cmp__ = self._set.__cmp__
        self.__contains__ = self._set.__contains__
        self.__eq__ = self._set.__eq__
        self.__ge__ = self._set.__ge__
        # Not implementing getattribute
        self.__gt__ = self._set.__gt__
        self.__hash__ = self._set.__hash__
        # Not implementing iand (it can remove items)
        # Special wrapper for ior to trigger events
        # Not implementing isub (it can remove items)
        self.__iter__ = self._set.__iter__
        # Not implementing ixor (it can remove items)
        self.__le__ = self._set.__le__
        self.__len__ = self._set.__len__
        self.__lt__ = self._set.__lt__
        self.__ne__ = self._set.__ne__
        self.__or__ = self._set.__or__
        self.__rand__ = self._set.__rand__
        # Special implementation of repr
        self.__ror__ = self._set.__ror__
        self.__rsub__ = self._set.__rsub__
        self.__rxor__ = self._set.__rxor__
        self.__sub__ = self._set.__sub__
        self.__xor__ = self._set.__xor__
        
        # Special implementation of add to trigger events
        # Not implementing clear
        self.copy = self._set.copy
        self.difference = self._set.difference
        # Not implementing difference_update (it removes items)
        # Not implementing discard (it removes items)
        self.intersection = self._set.intersection
        # Not implementing intersection_update (it removes items)
        self.issubset = self._set.issubset
        self.issuperset = self._set.issuperset
        # Not implementing pop
        # Not implementing remove
        self.symmetric_difference = self._set.symmetric_difference
        # Not implementing symmetric_difference_update
        self.union = self._set.union
        # Special implementation of update to trigger events
        
    def update(self, y):
        """Add all the elements of an iterable y to the current set.  If any of
        these elements were not already present, they will be broadcast to all
        other users."""
        s = set(y)
        d = s - self._set
        if len(d) > 0:
            self._set.update(d)
            self._send(d)
    
    __ior__ = update
    
    def add(self, y):
        """ Add the single element y to the current set.  If y is not already
        present, it will be broadcast to all other users."""
        if y not in self._set:
            self._set.add(y)
            self._send((y,))
    
    def _send(self, els):
        self._handler.send(dbus.Array([self._trans(el, True) for el in els]))
    
    def _net_update(self, y):
        s = set(y)
        d = s - self._set
        if len(d) > 0:
            self._set.update(d)
            self._trigger(d)
    
    def receive_message(self, msg):
        self._net_update((self._trans(el, False) for el in msg))
    
    def get_history(self):
        return dbus.Array([self._trans(el, True) for el in self._set])
    
    add_history = receive_message
    
    def register_listener(self, L):
        """Register a listener L(diffset).  Every time another user adds items
        to the set, L will be called with the set of new items."""
        self._listeners.append(L)
        L(self._set.copy())
    
    def _trigger(self, s):
        for L in self._listeners:
            L(s)
    
    def __repr__(self):
        return 'AddOnlySet(' + repr(self._handler) + ', ' + repr(self._set) + ', ' + repr(self._trans) + ')'

class AddOnlySortedSet:
    """ AddOnlySortedSet is much like AddOnlySet, only backed by a ListSet, which
    provides a set for objects that are ordered under cmp().  Items are maintained
    in order.  This approach is most useful in cases where each item is a message,
    and the messages are subject to a time-like ordering.  Messages may still
    arrive out of order, but they will be stored in the same order on each
    computer.
    """
    def __init__(self, handler, initset = (), translator=empty_translator):
        self._logger = logging.getLogger('dobject.AddOnlySortedSet')
        self._set = ListSet(initset)
        
        self._lock = threading.Lock()

        self._trans = translator
        self._listeners = []  #This must be done before registering with the handler

        self._handler = handler
        self._handler.register(self)
        
        self.__and__ = self._set.__and__
        self.__contains__ = self._set.__contains__
        # No self.__delitem__
        self.__eq__ = self._set.__eq__
        self.__ge__ = self._set.__ge__
        # Not implementing getattribute
        self.__getitem__ = self._set.__getitem__
        self.__gt__ = self._set.__gt__
        # Not implementing iand (it can remove items)
        # Special wrapper for ior to trigger events
        # Not implementing isub (it can remove items)
        self.__iter__ = self._set.__iter__
        # Not implementing ixor (it can remove items)
        self.__le__ = self._set.__le__
        self.__len__ = self._set.__len__
        self.__lt__ = self._set.__lt__
        self.__ne__ = self._set.__ne__
        self.__or__ = self._set.__or__
        self.__rand__ = self._set.__rand__
        # Special implementation of repr
        self.__ror__ = self._set.__ror__
        self.__rsub__ = self._set.__rsub__
        self.__rxor__ = self._set.__rxor__
        self.__sub__ = self._set.__sub__
        self.__xor__ = self._set.__xor__
        
        # Special implementation of add to trigger events
        # Not implementing clear
        self.copy = self._set.copy
        self.difference = self._set.difference
        # Not implementing difference_update (it removes items)
        # Not implementing discard (it removes items)
        self.first = self._set.first
        self.headset = self._set.headset
        self.index = self._set.index
        self.intersection = self._set.intersection
        # Not implementing intersection_update (it removes items)
        self.issubset = self._set.issubset
        self.issuperset = self._set.issuperset
        self.last = self._set.last
        # Not implementing pop
        self.position = self._set.position
        # Not implementing remove
        self.subset = self._set.subset
        self.symmetric_difference = self._set.symmetric_difference
        # Not implementing symmetric_difference_update
        self.tailset = self._set.tailset
        self.union = self._set.union
        # Special implementation of update to trigger events
        
    def update(self, y):
        """Add all the elements of an iterable y to the current set.  If any of
        these elements were not already present, they will be broadcast to all
        other users."""
        d = ListSet(y)
        d -= self._set
        if len(d) > 0:
            self._set.update(d)
            self._send(d)
    
    __ior__ = update
    
    def add(self, y):
        """ Add the single element y to the current set.  If y is not already
        present, it will be broadcast to all other users."""
        if y not in self._set:
            self._set.add(y)
            self._send((y,))
    
    def _send(self, els):
        self._handler.send(dbus.Array([self._trans(el, True) for el in els]))
    
    def _net_update(self, y):
        d = ListSet()
        d._list = y
        d -= self._set
        if len(d) > 0:
            self._set |= d
            self._trigger(d)
    
    def receive_message(self, msg):
        self._net_update([self._trans(el, False) for el in msg])
    
    def get_history(self):
        return dbus.Array([self._trans(el, True) for el in self._set._list])
    
    add_history = receive_message
    
    def register_listener(self, L):
        """Register a listener L(diffset).  Every time another user adds items
        to the set, L will be called with the set of new items as a SortedSet."""
        self._listeners.append(L)
        L(self._set.copy())
    
    def _trigger(self, s):
        for L in self._listeners:
            L(s)
    
    def __repr__(self):
        return 'AddOnlySortedSet(' + repr(self._handler) + ', ' + repr(self._set) + ', ' + repr(self._trans) + ')'
        
        
def CausalHandler():
    """The CausalHandler is analogous to the UnorderedHandler, in that it
    presents an interface with which to build a wide variety of objects with
    distributed state.  The CausalHandler is different from the Unordered in two
    ways:
    
    1. The send() method of an CausalHandler returns an index, which must be
    stored by the CausalObject in connection with the information that was sent.
    This index is a universal, fully-ordered, strictly causal identifier
    for each message.
    
    2. A CausalObject's receive_message method takes two arguments: the message
    and its index.
    
    As a convenience, there is also
    
    3. A get_index() method, which provides a new index on each call, always
    higher than all previous indexes.
    
    CausalObjects are responsible for including index information in the
    return value of get_history, and processing index information in add_history.
    
    It is noteworthy that CausalHandler is in fact implemented on _top_ of
    UnorderedHandler.  The imposition of ordering does not require lower-level
    access to the network.  This fact of implementation may change in the
    future, but CausalObjects will not be able to tell the difference.
    """
    _max64 = 2**64

    def __init__(self, name, tube_box):
        self._unordered = UnorderedObject(name, tube_box)
        self._counter = 0
        
        self._object = None
    
    def register(self, obj):
        self._object = obj
        self._unordered.register(self)
    
    def get_index(self):
        """get_index returns a new index, higher than all previous indexes.
        The primary reason to use get_index is if you wish two know the index
        of an item _before_ calling send()"""
        self._counter += 1
        return (self._counter, random.randrange(0, CausalHandler._max64))
    
    def index_trans(self, index, pack):
        """index_trans is a standard serialization translator for the index
        format. Thanks to this translator, a CausalObject can and should treat
        each index as an opaque, comparable object."""
        if pack:
            return dbus.Tuple((dbus.UInt64(index[0]), dbus.UInt64(index[1])), 'tt')
        else:
            return (int(index[0]), int(index[1]))
    
    def send(self, msg, index=None):
        """send() broadcasts a message to all other participants.  If called
        with one argument, send() broadcasts that message, along with a new
        index, and returns the index.  If called with two arguments, the second
        may be an index, which will be used for this message.  The index must
        have been acquired using get_index().  In this case, the index must be
        acquired immediately prior to calling send().  Otherwise, another
        message may arrive in the interim, causing a violation of causality."""
        if index is None:
            index = self.get_index()
        self._unordered.send(dbus.Tuple((msg, self.index_trans(index, True))))
        return index
    
    def receive_message(self, msg):
        m = msg[0]
        index = self.index_trans(msg[1], False)
        self._counter = max(self._counter, index[0])
        self._object.receive_message(m, index)
    
    def add_history(self, hist):
        h = hist[0]
        index = self.index_trans(hist[1], False)
        self._counter = max(self._counter, index[0])
        self._object.add_history(h)
    
    def get_history(self):
        h = self._object.get_history()
        hist = dbus.Tuple((h, self.index_trans(self.get_index(), True)))
        return

class CausalDict:
    """NOTE: CausalDict is UNTESTED.  Other things may be buggy, but CausalDict
    PROBABLY DOES NOT WORK.
    
    CausalDict is a distributed version of a Dict (hash table).  All users keep
    a copy of the entire table, so this is not a "Distributed Hash Table"
    according to the terminology of the field.
    
    CausalDict permits all Dict operations, including removing keys and
    modifying the value of existing keys.  This would not be possible using an
    Unordered approach, because two value assignments to the same key could
    arrive in different orders for different users, leaving them in different
    states at quiescence.
    
    To solve this problem, every assignment and removal is given a monotonically
    increasing unique index, and whenever there is a conflict, the higher-index
    operation wins.
    
    One side effect of this design is that deleted keys cannot be forgotten. If
    an assignment operation is received whose index is lower than
    the deletion's, then that assignment is considered obsolete and must not be
    executed.
    
    To provide a mechanism for reducing memory usage, the clear() method has
    been interpreted to remove not only all entries received so far, but also
    all entries that will ever be received with index less than the current
    index.
    """
    ADD = 0
    DELETE = 1
    CLEAR = 2

    def __init__(self, handler, initdict=(), key_translator=empty_translator, value_translator=empty_translator):
        self._handler = handler
        self._dict = dict(initdict)
        self._clear = self._handler.get_index() #this must happen before index_dict initialization, so that self._clear is less than any index in index_dict
        self._index_dict = dict(((k, self._handler.get_index()) for k in initdict))
        
        self._listeners = []
        
        self._key_trans = key_translator
        self._val_trans = value_translator
        
        self.__contains__ = self._dict.__contains__
        #Special __delitem__
        self.__eq__ = self._dict.__eq__
        self.__ge__ = self._dict.__ge__
        self.__getitem__ = self._dict.__getitem__
        self.__gt__ = self._dict.__gt__
        self.__le__ = self._dict.__le__
        self.__len__ = self._dict.__len__
        self.__lt__ = self._dict.__lt__
        self.__ne__ = self._dict.__ne__
        # special __setitem__
        
        #Special clear
        self.copy = self._dict.copy
        self.get = self._dict.get
        self.has_key = self._dict.has_key
        self.items = self._dict.items
        self.iteritems = self._dict.iteritems
        self.iterkeys = self._dict.iterkeys
        self.itervalues = self._dict.itervalues
        self.keys = self._dict.keys
        #Special pop
        #Special popitem
        #special setdefault
        #special update
        self.values = self._dict.values
        
        self._handler.register(self)
    
    def __delitem__(self, key):
        """Same as for dict"""
        del self._dict[key]
        n = self._handler.send(((dbus.Int32(CausalDict.DELETE), self._key_trans(key, True))))
        self._index_dict[key] = n
    
    def __setitem__(self, key, value):
        """Same as for dict"""
        self._dict[key] = value
        n = self._handler.send(dbus.Array([(dbus.Int32(CausalDict.ADD), self._key_trans(key, True), self._val_trans(value, True))]))
        self._index_dict[key] = n
    
    def clear(self):
        """Same as for dict"""
        self._dict.clear()
        self._index_dict.clear()
        n = self._handler.send(dbus.Array([(dbus.Int32(CausalDict.CLEAR))]))
        self._clear = n
    
    def pop(self, key, x=None):
        """Same as for dict"""
        t = (key in self._dict)
        if x is None:
            r = self._dict.pop(key)
        else:
            r = self._dict.pop(key, x)
        
        if t:
            n = self._handler.send(dbus.Array([(dbus.Int32(CausalDict.DELETE), self._key_trans(key, True))]))
            self._index_dict[key] = n
        
        return r
    
    def popitem(self):
        """Same as for dict"""
        p = self._dict.popitem()
        key = p[0]
        n = self._handler.send(dbus.Array([(dbus.Int32(CausalDict.DELETE), self._key_trans(key, True))]))
        self._index_dict[key] = n
        return p
    
    def setdefault(self, key, x):
        """Same as for dict"""
        if key not in self._dict:
            self._dict[key] = x
            n = self._handler.send(dbus.Array([(dbus.Int32(CausalDict.ADD), self._key_trans(key, True), self._val_trans(value, True))]))
        self._index_dict[key] = n
    
    def update(*args,**kargs):
        """Same as for dict"""
        d = dict()
        d.update(*args,**kargs)
        newpairs = []
        for p in d.items():
            if (p[0] not in self._dict) or (self._dict[p[0]] != p[1]):
                newpairs.append(p)
                self._dict[p[0]] = p[1]
        n = self._handler.send(dbus.Array([(dbus.Int32(CausalDict.ADD), self._key_trans(p[0], True), self._val_trans(p[1], True)) for p in newpairs]))
        
        for p in newpairs:
            self._index_dict[p[0]] = n
    
    def receive_message(self, msg, n):
        if n > self._clear:
            a = dict()
            r = dict()
            for m in msg:
                flag = int(m[0]) #don't know length of m without checking flag
                if flag == CausalDict.ADD:
                    key = self._key_trans(m[1], False)
                    if (key not in self._index_dict) or (self._index_dict[key] < n):
                        val = self._val_trans(m[2], False)
                        if key in self._dict:
                            r[key] = self._dict[key]
                        self._dict[key] = val
                        a[key] = val
                        self._index_dict[key] = n
                elif flag == CausalDict.DELETE:
                    key = self._key_trans(m[1], False)
                    if key not in self._index_dict:
                        self._index_dict[key] = n
                    elif (self._index_dict[key] < n):
                        self._index_dict[key] = n
                        if key in self._dict:
                            r[key] = self._dict[key]
                            del self._dict[key]
                elif flag == CausalDict.CLEAR:
                    self._clear = n
                    for (k, ind) in self._index_dict.items():
                        if ind < self._clear:
                            del self._index_dict[k]
                            if k in self._dict:
                                r[k] = self._dict[k]
                                del self._dict[k]
            if (len(a) > 0) or (len(r) > 0):
                self._trigger(a,r)

    def get_history(self):
        c = self._handler.index_trans(self._clear, True)
        d = dbus.Array([(self._key_trans(p[0], True), self._val_trans(p[1], True)) for p in self._dict.items()])
        i = dbus.Array([(self._key_trans(p[0], True), self._handler.index_trans(p[1], True)) for p in self._index_dict.items()])
        return dbus.Tuple((c,d,i))
    
    def add_history(self, hist):
        c = self._handler.index_trans(hist[0], False)
        d = dict(((self._key_trans(p[0], False), self._val_trans(p[1], False)) for p in hist[1]))
        i = [(self._key_trans(p[0], False), self._handler.index_trans(p[1], False)) for p in hist[2]]
        
        a = dict()
        r = dict()
        
        if c > self._clear:
            self._clear = c
            for (k, n) in self._index_dict.items():
                if n < self._clear:
                    del self._index_dict[k]
                    if k in self._dict:
                        r[k] = self._dict[k]
                        del self._dict[k]
        
        k_changed = []
        for (k, n) in i:
            if (((k not in self._index_dict) and (n > self._clear)) or
                ((k in self._index_dict) and (n > self._index_dict[k]))):
                k_changed.append(k)
                self._index_dict[k] = n
        
        for k in k_changed:
            if k in d:
                if (k in self._dict) and (self._dict[k] != d[k]):
                    r[k] = self._dict[k]
                    a[k] = d[k]
                elif k not in self._dict:
                    a[k] = d[k]
                self._dict[k] = d[k]
            else:
                if k in self._dict:
                    r[k] = self._dict[k]
                    del self._dict[k]
        
        if (len(a) > 0) or (len(r) > 0):
            self._trigger(a,r)
        
    def register_listener(self, L):
        """Register a change-listener L.  Whenever another user makes a change
        to this dict, L will be called with L(dict_added, dict_removed).  The
        two arguments are the dict of new entries, and the dict of entries that
        have been deleted or overwritten."""
        self._listeners.append(L)
        L(self._dict.copy(), dict())
    
    def _trigger(self, added, removed):
        for L in self._listeners:
            L(added, removed)
