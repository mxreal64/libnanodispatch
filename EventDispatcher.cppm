// Copyright (C) 2026 mxreal64
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://gnu.org>.


export module EventDispatcher;

import <cstdint>;
import <cstddef>;
import <atomic>;
import <new>;
import <utility>;

export template <typename EventType>
struct alignas(64) EventSlot {
    // Store as raw storage to avoid forced default construction
    alignas(alignof(EventType)) std::byte event_storage[sizeof(EventType)];
    alignas(64) std::atomic<uint64_t> sequence{0};
};

export template <typename EventType, std::size_t Capacity>
class NanosecondDispatcher {
private:
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
    static constexpr std::size_t Mask = Capacity - 1;

    alignas(64) EventSlot<EventType> ring_buffer_[Capacity];
    alignas(64) std::atomic<uint64_t> producer_sequence_{0};
    alignas(64) std::atomic<uint64_t> consumer_sequence_{0};

    static inline void cpu_relax() noexcept {
        #if defined(__x86_64__) || defined(_M_X64)
        __builtin_ia32_pause();
        #elif defined(__aarch64__)
        asm volatile("yield" ::: "memory");
        #endif
    }

public:
    NanosecondDispatcher() {
        for (std::size_t i = 0; i < Capacity; ++i) {
            ring_buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~NanosecondDispatcher() {
        // Safe explicit destruction of any remaining unconsumed elements
        uint64_t head = consumer_sequence_.load(std::memory_order_relaxed);
        uint64_t tail = producer_sequence_.load(std::memory_order_relaxed);
        for (uint64_t i = head; i < tail; ++i) {
            auto* slot = &ring_buffer_[i & Mask];
            std::destroy_at(reinterpret_cast<EventType*>(slot->event_storage));
        }
    }

    NanosecondDispatcher(const NanosecondDispatcher&) = delete;
    NanosecondDispatcher& operator=(const NanosecondDispatcher&) = delete;

    template <typename... Args>
    void publish(Args&&... args) noexcept {
        uint64_t ticket = producer_sequence_.load(std::memory_order_relaxed);
        EventSlot<EventType>* slot = nullptr;

        while (true) {
            slot = &ring_buffer_[ticket & Mask];
            uint64_t seq = slot->sequence.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(ticket);

            if (diff == 0) {
                if (producer_sequence_.compare_exchange_weak(ticket, ticket + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else {
                cpu_relax();
                ticket = producer_sequence_.load(std::memory_order_relaxed);
            }
        }

        ::new (static_cast<void*>(slot->event_storage)) EventType(std::forward<Args>(args)...);
        slot->sequence.store(ticket + 1, std::memory_order_release);
    }

    template <typename EventHandler>
    void consume_next(EventHandler&& handler) noexcept {
        uint64_t ticket = consumer_sequence_.load(std::memory_order_relaxed);
        EventSlot<EventType>* slot = nullptr;

        while (true) {
            slot = &ring_buffer_[ticket & Mask];
            uint64_t seq = slot->sequence.load(std::memory_order_acquire);
            int64_t diff = static_cast<int64_t>(seq) - static_cast<int64_t>(ticket + 1);

            if (diff == 0) {
                if (consumer_sequence_.compare_exchange_weak(ticket, ticket + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else {
                cpu_relax();
                ticket = consumer_sequence_.load(std::memory_order_relaxed);
            }
        }

        auto* item = reinterpret_cast<EventType*>(slot->event_storage);
        std::forward<EventHandler>(handler)(*item);
        std::destroy_at(item); 
        
        slot->sequence.store(ticket + Capacity, std::memory_order_release);
    }
};
