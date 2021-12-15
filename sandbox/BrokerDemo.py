# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import os
import random
import threading
import time

from queueos import Broker, Environment, FunctionFactory, Request

# This demo simulates a random workload, turn on and off the
# availability of adaptors, and change the number of workers. The QoS
# rules are expressed in the file 'broker.rules' in the same directory.


USERS = ["alice", "bob", "carlos", "david", "erin", "frank"]
DATASETS = ["dataset-1", "dataset-2", "dataset-3"]
ADAPTORS = ["adaptor1", "adaptor2"]


environment = Environment()


FunctionFactory.register_function(
    "dataset",
    lambda context, *args: context.request.dataset,
)
FunctionFactory.register_function(
    "adaptor",
    lambda context, *args: context.request.adaptor,
)


class DemoRequest(Request):
    def __init__(self, user, dataset, adaptor):
        super().__init__()
        self.user = user
        self.dataset = dataset
        self.adaptor = adaptor
        self.cost = (0, 0)

    def execute(self):

        sleep = random.randint(0, 20)

        print(self, f"Running for {sleep} seconds")
        time.sleep(sleep)

        if random.randint(1, 10) == 1:
            raise Exception(f"{self} failed!!")

    def __repr__(self):
        return f"R-{self.id}-{self.user}-{self.dataset}-{self.adaptor}"


broker = Broker(
    os.path.join(os.path.dirname(__file__), "broker.rules"),
    4,
    environment,
)

RUN_UPDATE = True


def update_config():
    while broker.known_requests:
        time.sleep(20)
        adaptor = "adaptor1" if random.randint(0, 1) else "adaptor2"
        if random.randint(0, 1):
            environment.enable_resource(adaptor)
        else:
            environment.disable_resource(adaptor)

        broker.set_number_of_workers(random.randint(0, 5))
        print("----------------------")
        broker.status()
        print("----------------------")


broker.pause()

for j in range(40):
    broker.enqueue(
        DemoRequest(
            user=random.choice(USERS),
            dataset=random.choice(DATASETS),
            adaptor=random.choice(ADAPTORS),
        )
    )


threading.Thread(target=update_config, daemon=True).start()
broker.resume()


print("END OF WORK - FLUSHING")
broker.shutdown()
print("THE END")
