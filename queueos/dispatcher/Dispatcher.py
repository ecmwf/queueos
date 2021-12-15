# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import threading

from queueos.broker.Request import Status


class Worker:
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

    def __call__(self):

        while True:
            request = self.dispatcher.next_request()
            if request is None:
                break

            self.dispatcher.started(request)

            if request.canceled:
                self.dispatcher.failed(request, request.canceled)
                continue

            try:
                request.execute()
                self.dispatcher.complete(request)
            except Exception as e:
                print("!!! FAILED request:", e)
                self.dispatcher.failed(request, e)


class Dispatcher:
    def __init__(self, number_of_workers, picker, observer, environment):
        """

        Args:
            number_of_workers ([type]): initial number of worker threads
            picker ([type]): the object that is responsible for selecting the next request to be run
            observer ([type]):an object that in notified on certain events such as the starting and ending of requests
            environment ([type]):
        """
        self.picker = picker
        self.observer = observer
        self.queue = []
        self.condition = threading.Condition()
        self.number_of_workers = 0
        self.known_requests = []
        self.number_of_active_requests = 0
        self.paused = False

        environment.add_observer(self)
        self.set_number_of_workers(number_of_workers)

    def set_number_of_workers(self, number_of_workers):
        """Change the number of workers.

        This method will either stop some workers or
        start ones. Any worker will finish their work before stopping, so
        that change may take a while to take effect.
        """
        with self.condition:
            while self.number_of_workers < number_of_workers:
                worker = Worker(self)
                threading.Thread(target=worker, daemon=True).start()
                self.number_of_workers += 1

            while self.number_of_workers > number_of_workers:
                # Enqueuing None will stop the worker that selects that request
                self.enqueue(None)
                self.number_of_workers -= 1

            self.condition.notify_all()

    def enqueue(self, request):
        """Add a request to the Dispatcher queue. If the request is 'None', this
        means stop the worker that will select that None.
        """
        with self.condition:
            if request is not None:
                request.dispatcher = self
                request.status = Status.QUEUED
                self.known_requests.append(request)
            self.queue.append(request)
            self.condition.notify_all()

    def next_request(self):
        """This method is called by the worker threads to get the next request to
        execute. Returns the next request to be run, or 'None'. In this case, the worker
        thread will terminate.
        """
        while True:

            with self.condition:

                while len(self.queue) == 0 or self.paused:
                    self.condition.wait()

                try:
                    # This means stop the thread
                    i = self.queue.index(None)
                    self.queue.pop(i)
                    self.condition.notify_all()
                    return None
                except ValueError:
                    pass

                request = self.picker.pick(self.queue)
                if request is not None:
                    self.condition.notify_all()
                    return request

                # The queue is not empty, by there are no candidates selected by
                # the Picker, wait for some change
                self.condition.wait()

    def started(self, request):
        """Called by a worker upon start of a request"""
        with self.condition:
            request.status = Status.ACTIVE
            self.observer.notify_start_of_request(request)

            self.number_of_active_requests += 1
            self.condition.notify_all()

    def failed(self, request, error):
        """Called by a worker upon failure of a request"""
        with self.condition:
            request.error = error
            request.status = Status.ABORTED
            self.observer.notify_end_of_request(request)

            self.number_of_active_requests -= 1
            self.known_requests.remove(request)
            self.condition.notify_all()

    def complete(self, request):
        """Called by a worker upon successful completion of a request"""
        with self.condition:
            request.status = Status.COMPLETE
            self.observer.notify_end_of_request(request)
            self.number_of_active_requests -= 1
            self.known_requests.remove(request)
            self.condition.notify_all()

    def wait_for_all_requests(self):
        """Wait for the active and queued requests to complete."""
        print("Waiting....")
        with self.condition:
            assert self.number_of_workers

            for i, r in enumerate(self.known_requests):
                print("Waiting", i, r, r.status)

            while len(self.known_requests) > 0:
                print(
                    "wait_for_all_requests queue={} active={} workers={} known={}".format(
                        len(self.queue),
                        self.number_of_active_requests,
                        self.number_of_workers,
                        len(self.known_requests),
                    )
                )
                self.condition.wait()

        print("Done waiting....")

    def shutdown(self):
        """Wait for all requests to complete and stop all the worker threads"""
        print("Shutdown....")
        self.wait_for_all_requests()
        self.set_number_of_workers(0)

    def pause(self):
        with self.condition:
            assert not self.paused
            self.paused = True
            self.condition.notify_all()

    def resume(self):
        with self.condition:
            assert self.paused
            self.paused = False
            self.condition.notify_all()

    def notify_environment_changed(self):
        """Called by the environment when the status of a resource is changed"""
        with self.condition:
            self.condition.notify_all()
