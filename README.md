# libnanodispatch

A blazing-fast, lock-free, zero-allocation Multi-Producer Multi-Consumer (MPMC) event dispatcher utilizing modern **C++23 Modules**. 

`libnanodispatch` implements a high-throughput, low-latency sequence-barrier ring buffer (inspired by the LMAX Disruptor architecture). It features raw memory storage to bypass unnecessary constructor steps, strict RAII element management, and localized cross-platform CPU spin-relax assembly primitives.

## Key Features

* **Multi-Producer Multi-Consumer Layout**: Handles simultaneous high-volume event broadcasting and intake from multiple execution threads without standard lock overhead.
* **Zero Allocation on Publication**: Avoids heap allocation by writing events directly into pre-allocated memory slots via placement `new`.
* **Hardware Spin Optimization**: Leverages specialized assembly macros (`pause` on x86_64, `yield` on AArch64) to maximize instruction line throughput during thread loops.
* **Symmetric Sequence Isolation**: Avoids hardware cache invalidation by padding critical sequence limits out to 64-byte structural bounds (`alignas(64)`).

## Installation & Setup

Ensure you are utilizing an environment with native module translation capabilities active (such as **GCC 16.1+** or modern Clang).

### Build Flags
Add these parameters to compile your module layout:
```bash
g++ -std=c++23 -fmodules-ts --compile-std-module main.cpp -o app
```

## Architectural Example

```cpp
import std;
import EventDispatcher;

struct SystemEvent {
    uint32_t component_id;
    uint64_t timestamp;
};

int main() {
    // 1. Instantiates a dispatcher with 1024 array slots (must be a power of two)
    NanosecondDispatcher<SystemEvent, 1024> dispatcher;

    // 2. Publish an event natively into the ring buffer (Multi-Producer Safe)
    dispatcher.publish(42, 1716843600ULL);

    // 3. Process the event payload via a callback handler (Multi-Consumer Safe)
    dispatcher.consume_next([](const SystemEvent& event) {
        std::println("Event Processed from Component: {}", event.component_id);
    });

    return 0;
}
```

## Platform Architecture Verification

The dispatcher automatically maps optimized assembly instructions to your processor target structure at compile time:
* **x86_64 / Intel / AMD**: Evaluates to `__builtin_ia32_pause()` to stabilize pipeline pipeline stalls.
* **ARM64 / Apple Silicon / Graviton**: Evaluates to inline `yield` memory barriers.

## License

Copyright (C) 2026 mxreal64
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program. If not, see <https://gnu.org>.
