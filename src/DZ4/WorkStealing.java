package DZ4;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class WorkStealing {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable {
        void shutdown();
    }

    interface ShutdownableExecutor
            extends Shutdownable, Executor {
    }

    static class FixedThreadPoolExecutor
            implements ShutdownableExecutor {

        private final ExecutorService executor;

        FixedThreadPoolExecutor(int threadCount) {
            executor =
                    Executors.newFixedThreadPool(threadCount);
        }

        @Override
        public void execute(Runnable command) {
            executor.execute(command);
        }

        @Override
        public void shutdown() {
            executor.shutdown();

            try {
                while (!executor.awaitTermination(
                        1,
                        TimeUnit.DAYS
                )) {
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    static class ThreadPerTaskExecutor
            implements ShutdownableExecutor {

        private final List<Thread> threads =
                new ArrayList<>();

        @Override
        public void execute(Runnable command) {
            Thread thread = new Thread(command);
            threads.add(thread);
            thread.start();
        }

        @Override
        public void shutdown() {
            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class RoundRobinExecutor
            implements ShutdownableExecutor {

        protected static final Runnable EXIT_TASK = () -> {
        };

        protected final List<Thread> threads =
                new ArrayList<>();

        protected final List<BlockingDeque<Runnable>> tasks =
                new ArrayList<>();

        private boolean isShuttingDown = false;
        private int nextQueue = 0;

        RoundRobinExecutor(int threadCount) {
            for (int i = 0; i < threadCount; i++) {
                tasks.add(new LinkedBlockingDeque<>());
            }

            for (int i = 0; i < threadCount; i++) {
                threads.add(createThread(i));
            }

            for (Thread thread : threads) {
                thread.start();
            }
        }

        protected Thread createThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task;

                    try {
                        task = tasks.get(id).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    if (task == EXIT_TASK) {
                        return;
                    }

                    task.run();
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown) {
                throw new RejectedExecutionException(
                        "Executor is shutting down"
                );
            }

            tasks.get(nextQueue).addLast(command);

            nextQueue =
                    (nextQueue + 1) % threads.size();
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;

                    for (BlockingDeque<Runnable> queue : tasks) {
                        queue.addLast(EXIT_TASK);
                    }
                }
            }

            for (Thread thread : threads) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
    }

    static class WorkStealingExecutor
            extends RoundRobinExecutor {

        WorkStealingExecutor(int threadCount) {
            super(threadCount);
        }

        @Override
        protected Thread createThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task =
                            tasks.get(id).pollFirst();

                    if (task == null) {
                        task = stealTask(id);
                    }

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(
                                    1,
                                    TimeUnit.MILLISECONDS
                            );
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    if (task == null) {
                        continue;
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);

                        if (task == null) {
                            return;
                        }

                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1;
                 offset < tasks.size();
                 offset++) {

                int queueNumber =
                        (thiefId + offset) % tasks.size();

                BlockingDeque<Runnable> queue =
                        tasks.get(queueNumber);

                Runnable task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null) {
                    return task;
                }
            }

            return null;
        }
    }

    static class Measurement {
        final long submissionTime;
        final long fullTime;

        Measurement(long submissionTime, long fullTime) {
            this.submissionTime = submissionTime;
            this.fullTime = fullTime;
        }
    }

    private static final int THREAD_NUMBER = 10;
    private static final int TASK_NUMBER = 1_000;
    private static final int TARGET_FULL_TIME = 2_000;

    private static final int MEAN_TASK_TIME =
            TARGET_FULL_TIME
                    * THREAD_NUMBER
                    / TASK_NUMBER;

    private static final int MAX_TASK_TIME =
            MEAN_TASK_TIME * 2 + 1;

    private static final long TARGET_TOTAL_TASK_TIME =
            (long) TARGET_FULL_TIME
                    * THREAD_NUMBER;

    private static final long RANDOM_SEED = 42;
    private static final double PARETO_SHAPE = 1.5;

    private static final int PARETO_MAX_TASK_TIME =
            MEAN_TASK_TIME * 100;

    private static final long BLACK_HOLE_MULTIPLIER =
            10_000;

    private static volatile long blackHoleResult;

    static void blackHole(long difficulty) {
        long result = 1;
        long iterations =
                difficulty * BLACK_HOLE_MULTIPLIER;

        for (long i = 0; i < iterations; i++) {
            result =
                    result * 1_664_525
                            + 1_013_904_223
                            + i;
        }

        blackHoleResult = result;
    }

    static Runnable createTask(
            int difficulty,
            boolean useBlackHole
    ) {
        if (useBlackHole) {
            return () -> blackHole(difficulty);
        }

        return () -> {
            try {
                Thread.sleep(difficulty);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    static int createTaskDifficulty(
            TaskDistribution distribution,
            int taskId,
            Random random
    ) {
        if (distribution == TaskDistribution.UNIFORM) {
            return random.nextInt(MAX_TASK_TIME);
        }

        if (distribution == TaskDistribution.PERIODIC) {
            if (taskId % THREAD_NUMBER == 0) {
                return MEAN_TASK_TIME * THREAD_NUMBER;
            }

            return 0;
        }

        double scale =
                MEAN_TASK_TIME
                        * (PARETO_SHAPE - 1)
                        / PARETO_SHAPE;

        double value =
                scale
                        / Math.pow(
                        1 - random.nextDouble(),
                        1 / PARETO_SHAPE
                );

        return (int) Math.min(
                Math.round(value),
                PARETO_MAX_TASK_TIME
        );
    }

    static int[] createTaskDifficulties(
            TaskDistribution distribution
    ) {
        Random random = new Random(RANDOM_SEED);
        int[] difficulties = new int[TASK_NUMBER];

        long totalDifficulty = 0;

        for (int taskId = 0;
             taskId < TASK_NUMBER;
             taskId++) {

            difficulties[taskId] =
                    createTaskDifficulty(
                            distribution,
                            taskId,
                            random
                    );

            totalDifficulty += difficulties[taskId];
        }

        double scale =
                (double) TARGET_TOTAL_TASK_TIME
                        / totalDifficulty;

        double remainder = 0;
        long normalizedTotal = 0;

        for (int taskId = 0;
             taskId < TASK_NUMBER;
             taskId++) {

            double scaledDifficulty =
                    difficulties[taskId]
                            * scale
                            + remainder;

            difficulties[taskId] =
                    (int) scaledDifficulty;

            remainder =
                    scaledDifficulty
                            - difficulties[taskId];

            normalizedTotal += difficulties[taskId];
        }

        int taskId = 0;

        while (normalizedTotal
                < TARGET_TOTAL_TASK_TIME) {

            difficulties[taskId % TASK_NUMBER]++;
            normalizedTotal++;
            taskId++;
        }

        taskId = 0;

        while (normalizedTotal
                > TARGET_TOTAL_TASK_TIME) {

            int index = taskId % TASK_NUMBER;

            if (difficulties[index] > 0) {
                difficulties[index]--;
                normalizedTotal--;
            }

            taskId++;
        }

        return difficulties;
    }

    static List<Runnable> createTasks(
            TaskDistribution distribution,
            boolean useBlackHole
    ) {
        int[] difficulties =
                createTaskDifficulties(distribution);

        List<Runnable> result =
                new ArrayList<>();

        for (int difficulty : difficulties) {
            result.add(
                    createTask(
                            difficulty,
                            useBlackHole
                    )
            );
        }

        return result;
    }

    static Measurement measureExecutor(
            ShutdownableExecutor executor,
            TaskDistribution distribution,
            boolean useBlackHole
    ) {
        List<Runnable> tasks =
                createTasks(
                        distribution,
                        useBlackHole
                );

        long start = System.nanoTime();

        for (Runnable task : tasks) {
            executor.execute(task);
        }

        long submissionEnd = System.nanoTime();

        executor.shutdown();

        long finish = System.nanoTime();

        return new Measurement(
                submissionEnd - start,
                finish - start
        );
    }

    static void printMeasurement(
            String name,
            ShutdownableExecutor executor,
            TaskDistribution distribution,
            boolean useBlackHole
    ) {
        Measurement result =
                measureExecutor(
                        executor,
                        distribution,
                        useBlackHole
                );

        System.out.println(name);

        System.out.printf(
                "Передача задач: %.3f мс%n",
                result.submissionTime / 1_000_000.0
        );

        System.out.printf(
                "Полное время: %.3f мс%n",
                result.fullTime / 1_000_000.0
        );
    }

    static void runMeasurements(boolean useBlackHole) {
        if (useBlackHole) {
            System.out.println();
            System.out.println("BLACK HOLE");
        } else {
            System.out.println();
            System.out.println("SLEEP");
        }

        for (TaskDistribution distribution
                : TaskDistribution.values()) {

            System.out.println();
            System.out.println(
                    "Распределение: " + distribution
            );

            printMeasurement(
                    "Thread per task",
                    new ThreadPerTaskExecutor(),
                    distribution,
                    useBlackHole
            );

            printMeasurement(
                    "Fixed thread pool",
                    new FixedThreadPoolExecutor(
                            THREAD_NUMBER
                    ),
                    distribution,
                    useBlackHole
            );

            printMeasurement(
                    "Round robin",
                    new RoundRobinExecutor(
                            THREAD_NUMBER
                    ),
                    distribution,
                    useBlackHole
            );

            printMeasurement(
                    "Work stealing",
                    new WorkStealingExecutor(
                            THREAD_NUMBER
                    ),
                    distribution,
                    useBlackHole
            );
        }
    }

    public static void main(String[] args) {
        runMeasurements(false);
        runMeasurements(true);

        System.out.println();
        System.out.println(
                "Black hole result: "
                        + blackHoleResult
        );
    }
}
/*
SLEEP

Распределение: UNIFORM
Thread per task
Передача задач: 77,951 мс
Полное время: 117,568 мс
Fixed thread pool
Передача задач: 2,464 мс
Полное время: 2070,789 мс
Round robin
Передача задач: 1,301 мс
Полное время: 2141,727 мс
Work stealing
Передача задач: 0,318 мс
Полное время: 2111,149 мс

Распределение: PERIODIC
Thread per task
Передача задач: 76,303 мс
Полное время: 301,380 мс
Fixed thread pool
Передача задач: 1,223 мс
Полное время: 2049,504 мс
Round robin
Передача задач: 0,507 мс
Полное время: 20415,852 мс
Work stealing
Передача задач: 0,407 мс
Полное время: 2052,701 мс

Распределение: PARETO
Thread per task
Передача задач: 82,286 мс
Полное время: 1552,795 мс
Fixed thread pool
Передача задач: 1,015 мс
Полное время: 2451,214 мс
Round robin
Передача задач: 0,300 мс
Полное время: 3248,154 мс
Work stealing
Передача задач: 0,294 мс
Полное время: 2117,850 мс

BLACK HOLE

Распределение: UNIFORM
Thread per task
Передача задач: 68,046 мс
Полное время: 165,307 мс
Fixed thread pool
Передача задач: 4,703 мс
Полное время: 65,030 мс
Round robin
Передача задач: 0,270 мс
Полное время: 67,301 мс
Work stealing
Передача задач: 0,206 мс
Полное время: 46,623 мс

Распределение: PERIODIC
Thread per task
Передача задач: 70,118 мс
Полное время: 110,641 мс
Fixed thread pool
Передача задач: 1,965 мс
Полное время: 56,738 мс
Round robin
Передача задач: 3,123 мс
Полное время: 450,291 мс
Work stealing
Передача задач: 1,828 мс
Полное время: 66,039 мс

Распределение: PARETO
Thread per task
Передача задач: 63,967 мс
Полное время: 110,646 мс
Fixed thread pool
Передача задач: 0,680 мс
Полное время: 59,366 мс
Round robin
Передача задач: 0,193 мс
Полное время: 69,027 мс
Work stealing
Передача задач: 0,176 мс
Полное время: 48,967 мс

Black hole result: -3605874101046893871
 */