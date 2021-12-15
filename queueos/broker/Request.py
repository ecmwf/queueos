# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import threading
import time

ID = 0
LOCK = threading.RLock()


class Status:

    UNKNOWN = "UNKNOWN"
    QUEUED = "QUEUED"
    SUBMITTED = "SUBMITTED"
    ACTIVE = "ACTIVE"
    ABORTED = "ABORTED"
    COMPLETE = "COMPLETE"


class Request:

    """
    * TODO: The self.startTime must be established when the request is added to the
    * Broker queue. The self.startTime must be persistent, i.e. it must be stored
    * in a database so that when we restart the Broker and re-populate its
    * queue, the original start time is preserved.
    """

    def __init__(self):
        with LOCK:
            global ID
            self.id = ID
            ID += 1

        self.canceled = None
        self.error = None
        self.status = Status.UNKNOWN

        self.start = time.time()
        self.dispatcher = None

    def execute(self):
        raise NotImplementedError("Please override this method")

    @property
    def age(self):
        """Returns the age of the request in seconds".

        Returns:
            float: Age in seconds.
        """
        return time.time() - self.start
