"""
Copyright 2008 Benjamin M. Schwartz

This file is LGPLv2+.  This file, dobject_helpers.py, is part of DObject.

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

import bisect

"""
dobject_helpers is a collection of functions and data structures that are useful
to DObject, but are not specific to DBus or networked applications.
"""

def merge(a, b, l=True, g=True, e=True):
    """Internal helper function for combining sets represented as sorted lists"""
    x = 0
    X = len(a)
    if X == 0:
        if g:
            return list(b)
        else:
            return []
    y = 0
    Y = len(b)
    if Y == 0:
        if l:
            return list(a)
        else:
            return []
    out = []
    p = a[x]
    q = b[y]
    while x < X and y < Y:
        if p < q:
            if l: out.append(p)
            x += 1
            if x < X: p = a[x]
        elif p > q:
            if g: out.append(q)
            y += 1
            if y < Y: q = b[y]
        else:
            if e: out.append(p)
            x += 1
            if x < X: p = a[x]
            y += 1
            if y < Y: q = b[y]       
    if x < X:
        if l: out.extend(a[x:])
    else:
        if g: out.extend(b[y:])
    return out

def merge_or(a,b):
    return merge(a,b, True, True, True)

def merge_xor(a,b):
    return merge(a, b, True, True, False)

def merge_and(a,b):
    return merge(a, b, False, False, True)

def merge_sub(a,b):
    return merge(a, b, True, False, False)

def kill_dupes(a): #assumes a is sorted
    """Internal helper function for removing duplicates in a sorted list"""
    prev = a[0]
    out = [prev]
    for i in xrange(1, len(a)):
        item = a[i]
        if item != prev:
            out.append(item)
            prev = item
    return out

class Comparable:
    """Currently, ListSet does not provide a mechanism for specifying a
    comparator.  Users who would like to specify a comparator other than the one
    native to the item may do so by wrapping the item in a Comparable.
    """
    def __init__(self, item, comparator):
        self.item = item
        self._cmp = comparator
    
    def __cmp__(self, other):
        return self._cmp(self.item, other)

class ListSet:
    """ListSet is a sorted set for comparable items.  It is inspired by the
    Java Standard Library's TreeSet.  However, it is implemented by a sorted
    list.  This implementation is much slower than a balanced binary tree, but
    has the distinct advantage that I can actually implement it.
    
    The methods of ListSet are all drawn directly from Python's set API,
    Python's list API, and Java's SortedSet API.
    """
    def __init__(self, seq=[]):
        L = list(seq)
        if len(L) > 1:
            L.sort()
            L = kill_dupes(L)
        self._list = L

    def __and__(self, someset):
        if someset.__class__ == self.__class__:
            L = merge_and(self._list, someset._list)
        else:
            L = []
            for x in self._list:
                if x in someset:
                    L.append(x)
        a = ListSet()
        a._list = L
        return a
    
    def __contains__(self, item):
        if len(self._list) == 0:
            return False
        if self._list[0] <= item <= self._list[-1]:
            a = bisect.bisect_left(self._list, item)
            return item == self._list[a]
        else:
            return False
    
    def __eq__(self, someset):
        if someset.__class__ == self.__class__:
            return self._list == someset._list
        else:
            return len(self.symmetric_difference(someset)) == 0
    
    def __ge__(self, someset):
        if someset.__class__ == self.__class__:
            return len(merge_or(self._list, someset._list)) == len(self._list)
        else:
            a = len(someset)
            k = 0
            for i in self._list:
                if i in someset:
                    k += 1
            return k == a
    
    def __gt__(self, someset):
        return (len(self) > len(someset)) and (self >= someset)

    def __iand__(self, someset):
        if someset.__class__ == self.__class__:
            self._list = merge_and(self._list, someset._list)
        else:
            L = []
            for i in self._list:
                if i in someset:
                    L.append(i)
            self._list = L
        return self
    
    def __ior__(self, someset):
        if someset.__class__ == self.__class__:
            self._list = merge_or(self._list, someset._list)
        else:
            self.update(someset)
        return self
    
    def __isub__(self, someset):
        if someset.__class__ == self.__class__:
            self._list = merge_sub(self._list, someset._list)
        else:
            L = []
            for i in self._list:
                if i not in someset:
                    L.append(i)
            self._list = L
        return self
    
    def __iter__(self):
        return self._list.__iter__()
    
    def __ixor__(self, someset):
        if someset.__class__ == self.__class__:
            self._list = merge_xor(self._list, someset._list)
        else:
            self.symmetric_difference_update(someset)
        return self
    
    def __le__(self, someset):
        if someset.__class__ == self.__class__:
            return len(merge_or(self._list, someset._list)) == len(someset._list)
        else:
            for i in self._list:
                if i not in someset:
                   return False
            return True
    
    def __lt__(self, someset):
        return (len(self) < len(someset)) and (self <= someset)
    
    def __ne__(self, someset):
        return not (self == someset)
    
    def __len__(self):
        return len(self._list)
    
    def __or__(self, someset):
        a = ListSet()
        if someset.__class__ == self.__class__:
            a._list = merge_or(self._list, someset._list)
        else:
            a._list = self._list
            a.update(someset)
        return a
    
    __rand__ = __and__
    
    def __repr__(self):
        return "ListSet(" + repr(self._list) +")"
    
    __ror__ = __or__
    
    def __rsub__(self, someset):
        if someset.__class__ == self.__class__:
            a = ListSet()
            a._list = merge_sub(someset._list, self._list)
        else:
            a = ListSet(someset)
            a._list = merge_sub(a._list, self._list)
        return a
    
    def __sub__(self, someset):
        a = ListSet()
        if someset.__class__ == self.__class__:
            a._list = merge_sub(self._list, someset._list)
        else:
            L = []
            for i in self._list:
                if i not in someset:
                    L.append(i)
            a._list = L
        return a
    
    def __xor__(self, someset):
        if someset.__class__ == self.__class__:
            a = ListSet()
            a._list = merge_xor(self._list, someset._list)
        else:
            a = self.symmetric_difference(someset)
        return a
    
    __rxor__ = __xor__
    
    def add(self, item):
        if (len(self._list) > 0) and (item <= self._list[-1]):
            a = bisect.bisect_left(self._list, item)
            if self._list[a] != item:
                self._list.insert(a, item)
        else:
            self._list.append(item)
    
    def clear(self):
        self._list = []
    
    def copy(self):
        a = ListSet()
        a._list = list(self._list) #shallow copy
        return a
    
    def difference(self, iterable):
        L = list(iterable)
        L.sort()
        a = ListSet()
        a._list = merge_sub(self._list, kill_dupes(L))
        return a
    
    def difference_update(self, iterable):
        L = list(iterable)
        L.sort()
        self._list = merge_sub(self._list, kill_dupes(L))
    
    def discard(self, item):
        if (len(self._list) > 0) and (item <= self._list[-1]):
            a = bisect.bisect_left(self._list, item)
            if self._list[a] == item:
                self._list.remove(a)
    
    def intersection(self, iterable):
        L = list(iterable)
        L.sort()
        a = ListSet()
        a._list = merge_and(self._list, kill_dupes(L))
    
    def intersection_update(self, iterable):
        L = list(iterable)
        L.sort()
        self._list = merge_and(self._list, kill_dupes(L))
    
    def issuperset(self, iterable):
        L = list(iterable)
        L.sort()
        m = merge_or(self._list, kill_dupes(L))
        return len(m) == len(self._list)
    
    def issubset(self, iterable):
        L = list(iterable)
        L.sort()
        L = kill_dupes(L)
        m = merge_or(self._list, L)
        return len(m) == len(L)
    
    def pop(self, i = None):
        if i == None:
            return self._list.pop()
        else:
            return self._list.pop(i)
        
    def remove(self, item):
        if (len(self._list) > 0) and (item <= self._list[-1]):
            a = bisect.bisect_left(self._list, item)
            if self._list[a] == item:
                self._list.remove(a)
                return
        raise KeyError("Item is not in the set")
    
    def symmetric_difference(self, iterable):
        L = list(iterable)
        L.sort()
        a = ListSet()
        a._list = merge_xor(self._list, kill_dupes(L))
        return a
    
    def symmetric_difference_update(self, iterable):
        L = list(iterable)
        L.sort()
        self._list = merge_xor(self._list, kill_dupes(L))
    
    def union(self, iterable):
        L = list(iterable)
        L.sort()
        a = ListSet()
        a._list = merge_or(self._list, kill_dupes(L))
    
    def update(self, iterable):
        L = list(iterable)
        L.sort()
        self._list = merge_or(self._list, kill_dupes(L))
    
    def __getitem__(self, key):
        if type(key) == int:
            return self._list.__getitem__(key)
        elif type(key) == slice:
            a = ListSet()
            L = self._list.__getitem__(key)
            if key.step < 0:
                L.reverse()
            a._list = L
            return a
    
    def __delitem__(self, key):
        self._list.__delitem__(key)
    
    def index(self, x, i=0, j=-1):
        if (len(self._list) > 0) and (x <= self._list[-1]):
            a = bisect.bisect_left(self._list, x, i, j)
            if self._list[a] == x:
                return a
        raise ValueError("Item not found")
    
    def position(self, x, i=0, j=-1):
        return bisect.bisect_left(self._list, x, i, j)
    
    def subset(self, x, y):
        a = bisect.bisect_left(self._list, x)
        b = bisect.bisect_left(self._list, y)
        s = ListSet()
        s._list = self._list[a:b]
        return s
    
    def first(self):
        return self._list[0]
    
    def last(self):
        return self._list[-1]
    
    def headset(self, x):
        a = bisect.bisect_left(self._list, x, i, j)
        return self[:a]
    
    def tailset(self, x):
        a = bisect.bisect_left(self._list, x, i, j)
        return self[a:]
