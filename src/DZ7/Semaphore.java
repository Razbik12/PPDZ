package DZ7;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class Semaphore {
    private final AtomicInteger permits;
    private final ConcurrentLinkedQueue<Thread> waiting = new ConcurrentLinkedQueue<>();

    public Semaphore() {
        this(5);
    }

    public Semaphore(int permits) {
        if (permits < 0) {
            throw new IllegalArgumentException();
        }
        this.permits = new AtomicInteger(permits);
    }

    public void acquire(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException();
        }

        Thread current = Thread.currentThread();
        waiting.add(current);
        boolean interrupted = false;

        while (true) {
            if (waiting.peek() == current) {
                int available = permits.get();
                if (available >= n && permits.compareAndSet(available, available - n)) {
                    waiting.poll();
                    wakeNext();
                    break;
                }
            }

            if (Thread.interrupted()) {
                interrupted = true;
            }
            LockSupport.park(this);
        }

        if (interrupted) {
            current.interrupt();
        }
    }

    public void release(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException();
        }

        permits.updateAndGet(available -> Math.addExact(available, n));
        wakeNext();
    }

    private void wakeNext() {
        Thread next = waiting.peek();
        if (next != null) {
            LockSupport.unpark(next);
        }
    }

    public static void main(String[] args) {
        Semaphore semaphore = new Semaphore(2);

        for (int i = 1; i <= 4; i++) {
            Thread thread = new Thread(() -> {
                semaphore.acquire(1);
                try {
                    System.out.println(Thread.currentThread().getName() + " entered");
                    Thread.sleep(300);
                    System.out.println(Thread.currentThread().getName() + " leaving");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    semaphore.release(1);
                }
            }, "Thread-" + i);
            thread.start();
        }
    }
}
