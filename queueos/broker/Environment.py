# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import threading
from functools import wraps

UNDEF = object()


def locked(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self.lock:
            return method(self, *args, **kwargs)

    return wrapped


class Environment:
    def __init__(self):
        self.lock = threading.RLock()
        self._enabled = {}
        self._values = {}
        self._observers = []

    @locked
    def set(self, resource, value):
        self._values[resource] = value
        self._notify_observers()

    @locked
    def get(self, resource, value=UNDEF):
        if value is UNDEF:
            return self._values[resource]
        else:
            return self._values.get(resource, value)

    @locked
    def resource_enabled(self, resource):
        return self._enabled.get(resource, True)

    @locked
    def enable_resource(self, resource):
        self._enabled[resource] = True
        self._notify_observers()

    @locked
    def disable_resource(self, resource):
        self._enabled[resource] = False
        self._notify_observers()

    @locked
    def add_observer(self, observer):
        self._observers.append(observer)

    @locked
    def remove_observer(self, observer):
        self._observers.remove(observer)

    def _notify_observers(self, *args, **kwargs):
        def notify(o):
            o.notify_environment_changed(*args, **kwargs)

        for o in self._observers:
            # Notify in a thread so we don't create deadlocks
            threading.Thread(target=notify, args=(o,), daemon=True)
