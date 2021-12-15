# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

from queueos.dispatcher.Dispatcher import Dispatcher
from queueos.qos.QoS import QoS


class Broker:
    """This is the Broker itself. Just a wrapper around a Dispatcher and a QoS"""

    def __init__(self, rules, number_of_workers, environment):
        self.qos = QoS(rules, environment)
        self.dispatcher = Dispatcher(number_of_workers, self.qos, self.qos, environment)

        self.qos.dump()

    def __del__(self):
        self.dispatcher.set_number_of_workers(0)

    def enqueue(self, request):
        assert request is not None
        self.dispatcher.enqueue(request)

    def set_number_of_workers(self, number_of_workers):
        self.dispatcher.set_number_of_workers(number_of_workers)

    def reload_rules(self):
        self.qos.reload_rules()

    def wait_for_all_requests(self):
        self.dispatcher.wait_for_all_requests()

    def shutdown(self):
        self.dispatcher.shutdown()

    def pause(self):
        self.dispatcher.pause()

    def resume(self):
        self.dispatcher.resume()

    def status(self, out=print):
        self.qos.status(self.dispatcher.known_requests, out)

    def reconfigure(self):
        self.qos.reconfigure()

    @property
    def known_requests(self):
        with self.dispatcher.condition:
            return len(self.dispatcher.known_requests)
