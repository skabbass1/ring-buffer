# Ring Buffer

A lockless ring buffer implementation using `Python 3.8's` [shared_memory](https://docs.python.org/3.9/library/multiprocessing.shared_memory.html) module.
The implementation follows the design described [here](https://ferrous-systems.com/blog/lock-free-ring-buffer/) and provides a sample producer/consumer app using the shared memory ring buffer


# TODO
Better handling and coordination of shared memory buffer cleanups across processes upon shutdown
