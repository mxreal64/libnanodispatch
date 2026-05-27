# libnanodispatch
A blazing-fast, lock-free, zero-allocation MPMC event dispatcher using C++23 modules. Implements an LMAX Disruptor-style sequence barrier ring buffer with raw storage tracking, custom RAII element destruction, and cross-platform assembly spin-relax primitives for sub-microsecond latency. pretty cool, right?
