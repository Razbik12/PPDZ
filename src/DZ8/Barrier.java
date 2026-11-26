package DZ8;

public class Barrier {
    private final int parties;
    private int arrived;
    private int generation;

    public Barrier(int parties) {
        if (parties <= 0) {
            throw new IllegalArgumentException();
        }
        this.parties = parties;
    }

    public synchronized void await() {
        int currentGeneration = generation;
        arrived++;

        if (arrived == parties) {
            arrived = 0;
            generation++;
            notifyAll();
            return;
        }

        boolean interrupted = false;
        while (currentGeneration == generation) {
            try {
                wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) {
        Barrier barrier = new Barrier(3);

        for (int i = 1; i <= 3; i++) {
            Thread thread = new Thread(() -> {
                String name = Thread.currentThread().getName();
                System.out.println(name + " before the barrier");
                barrier.await();
                System.out.println(name + " after the barrier");
            }, "Thread-" + i);
            thread.start();
        }
    }
}
