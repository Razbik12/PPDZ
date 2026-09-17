import java.util.concurrent.atomic.DoubleAdder;
public class Integral {
    public static final int STEPS = 10000000;
    public static final int THREADS = 6;
    // Класс для хранения общей суммы в Monitor.Поле acc используется всеми потоками.
    // Метод addToAcc синхронизирован, поэтому только один поток
    // может работать над acc одновременно
    static class Acc {
        volatile double acc = 0;
        synchronized public void addToAcc(double value) {
            acc += value;
        }
    }
    // 1 инт
    public static double func(double x) {
        return x * x;
    }
    // 2 инт
    public static double func2(double x) {
        return Math.sin(x) * Math.sin(x);
    }
    // все вычисления с первым интегралом
    public static Thread taskThread(int n, double a, double dx, double[] results) {
        return new Thread(() -> {
            int start = n * STEPS / THREADS;
            int finish = (n + 1) * STEPS / THREADS;
            double sum = 0;
            for (int i = start; i < finish; i++) {
                double x = a + (i + 0.5) * dx;
                sum += func(x);
            }
            results[n] = sum;
        });
    }
    public static double integrateSequential(double a, double b) {
        double dx = (b - a) / STEPS;
        double sum = 0;
        for (int i = 0; i < STEPS; i++) {
            double x = a + (i + 0.5) * dx;
            sum += func(x);
        }
        return sum * dx;
    }
    public static double integrateParallel(double a, double b)
            throws InterruptedException {
        double dx = (b - a) / STEPS;
        Thread[] threads = new Thread[THREADS]; //массив с потоками
        double[] results = new double[THREADS]; // массив для частичных результатов
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, a, dx, results);
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].join(); //ждем пока поток закончит операцию
        }
        double sum = 0;
        for (int i = 0; i < THREADS; i++) {
            sum += results[i];
        }
        return sum * dx;
    }
    public static Thread taskAtomicThread(
            int n,
            double a,
            double dx,
            DoubleAdder acc //туда все потоки отправляют результаты, чтобы сложить
    ) {
        return new Thread(() -> {
            int start = n * STEPS / THREADS;
            int finish = (n + 1) * STEPS / THREADS;
            for (int i = start; i < finish; i++) {
                double x = a + (i + 0.5) * dx;
                acc.add(func(x));
            }
        });
    }
    public static double integrateAtomic(double a, double b)
            throws InterruptedException {
        double dx = (b - a) / STEPS;
        Thread[] threads = new Thread[THREADS];
        DoubleAdder acc = new DoubleAdder();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread(i, a, dx, acc);
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }
        return acc.sum() * dx;
    }
    public static Thread taskMonitorThread(
            int n,
            double a,
            double dx,
            Acc acc
    ) {
        return new Thread(() -> {
            int start = n * STEPS / THREADS;
            int finish = (n + 1) * STEPS / THREADS;
            for (int i = start; i < finish; i++) {
                double x = a + (i + 0.5) * dx;
                acc.addToAcc(func(x));
            }
        });
    }
    public static double integrateMonitor(double a, double b)
            throws InterruptedException {
        double dx = (b - a) / STEPS;
        Thread[] threads = new Thread[THREADS];
        Acc acc = new Acc();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread(i, a, dx, acc);
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }
        return acc.acc * dx;
    }
    // абсолютно тоже самое но со вторым
    public static Thread taskThread2(
            int n,
            double a,
            double dx,
            double[] results
    ) {
        return new Thread(() -> {
            int start = n * STEPS / THREADS;
            int finish = (n + 1) * STEPS / THREADS;
            double sum = 0;
            for (int i = start; i < finish; i++) {
                double x = a + (i + 0.5) * dx;
                sum += func2(x);
            }
            results[n] = sum;
        });
    }
    public static double integrateSequential2(double a, double b) {
        double dx = (b - a) / STEPS;
        double sum = 0;
        for (int i = 0; i < STEPS; i++) {
            double x = a + (i + 0.5) * dx;
            sum += func2(x);
        }
        return sum * dx;
    }
    public static double integrateParallel2(double a, double b)
            throws InterruptedException {
        double dx = (b - a) / STEPS;
        Thread[] threads = new Thread[THREADS];
        double[] results = new double[THREADS];
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread2(i, a, dx, results);
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }
        double sum = 0;
        for (int i = 0; i < THREADS; i++) {
            sum += results[i];
        }
        return sum * dx;
    }
    public static Thread taskAtomicThread2(
            int n,
            double a,
            double dx,
            DoubleAdder acc
    ) {
        return new Thread(() -> {
            int start = n * STEPS / THREADS;
            int finish = (n + 1) * STEPS / THREADS;
            for (int i = start; i < finish; i++) {
                double x = a + (i + 0.5) * dx;
                acc.add(func2(x));
            }
        });
    }
    public static double integrateAtomic2(double a, double b)
            throws InterruptedException {
        double dx = (b - a) / STEPS;
        Thread[] threads = new Thread[THREADS];
        DoubleAdder acc = new DoubleAdder();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread2(i, a, dx, acc);
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }
        return acc.sum() * dx;
    }
    public static Thread taskMonitorThread2(
            int n,
            double a,
            double dx,
            Acc acc
    ) {
        return new Thread(() -> {
            int start = n * STEPS / THREADS;
            int finish = (n + 1) * STEPS / THREADS;
            for (int i = start; i < finish; i++) {
                double x = a + (i + 0.5) * dx;
                acc.addToAcc(func2(x));
            }
        });
    }
    public static double integrateMonitor2(double a, double b)
            throws InterruptedException {
        double dx = (b - a) / STEPS;
        Thread[] threads = new Thread[THREADS];
        Acc acc = new Acc();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread2(i, a, dx, acc);
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }
        return acc.acc * dx;
    }
    public static void main(String[] args)
            throws InterruptedException {
        //счет времени и вывод первого
        double a = 0;
        double b = 10;
        long start = System.nanoTime();
        double results =
                integrateSequential(a, b);
        long finish = System.nanoTime();
        double time =
                (double) (finish - start) / 1000000;
        long paralleStart = System.nanoTime();
        double resultsParallel =
                integrateParallel(a, b);
        long paralleFinish = System.nanoTime();
        double parallelTime =
                (double) (paralleFinish - paralleStart) / 1000000;
        long atomicStart = System.nanoTime();
        double resultsAtomic =
                integrateAtomic(a, b);
        long atomicFinish = System.nanoTime();
        double atomicTime =
                (double) (atomicFinish - atomicStart) / 1000000;
        long monitorStart = System.nanoTime();
        double resultsMonitor =
                integrateMonitor(a, b);
        long monitorFinish = System.nanoTime();
        double monitorTime =
                (double) (monitorFinish - monitorStart) / 1000000;
        System.out.println("FIRST INTEGRAL: x^2");
        System.out.println();
        System.out.println("Sequential result");
        System.out.println(results);
        System.out.println("Sequential time");
        System.out.println(time);
        System.out.println();
        System.out.println("Parallel result");
        System.out.println(resultsParallel);
        System.out.println("Parallel time");
        System.out.println(parallelTime);
        System.out.println();
        System.out.println("Atomic result");
        System.out.println(resultsAtomic);
        System.out.println("Atomic time");
        System.out.println(atomicTime);
        System.out.println();
        System.out.println("Monitor result");
        System.out.println(resultsMonitor);
        System.out.println("Monitor time");
        System.out.println(monitorTime);
        //счет времени и вывод второго
        double a2 = 0;
        double b2 = Math.PI;
        long start2 = System.nanoTime();
        double results2 =
                integrateSequential2(a2, b2);
        long finish2 = System.nanoTime();
        double time2 =
                (double) (finish2 - start2) / 1000000;
        long parallelStart2 = System.nanoTime();
        double resultsParallel2 =
                integrateParallel2(a2, b2);
        long parallelFinish2 = System.nanoTime();
        double parallelTime2 =
                (double) (parallelFinish2 - parallelStart2) / 1000000;
        long atomicStart2 = System.nanoTime();
        double resultsAtomic2 =
                integrateAtomic2(a2, b2);
        long atomicFinish2 = System.nanoTime();
        double atomicTime2 =
                (double) (atomicFinish2 - atomicStart2) / 1000000;
        long monitorStart2 = System.nanoTime();
        double resultsMonitor2 =
                integrateMonitor2(a2, b2);
        long monitorFinish2 = System.nanoTime();
        double monitorTime2 =
                (double) (monitorFinish2 - monitorStart2) / 1000000;
        System.out.println();
        System.out.println();
        System.out.println("SECOND INTEGRAL: sin^2(x)");
        System.out.println();
        System.out.println("Sequential result");
        System.out.println(results2);
        System.out.println("Sequential time");
        System.out.println(time2);
        System.out.println();
        System.out.println("Parallel result");
        System.out.println(resultsParallel2);
        System.out.println("Parallel time");
        System.out.println(parallelTime2);
        System.out.println();
        System.out.println("Atomic result");
        System.out.println(resultsAtomic2);
        System.out.println("Atomic time");
        System.out.println(atomicTime2);
        System.out.println();
        System.out.println("Monitor result");
        System.out.println(resultsMonitor2);
        System.out.println("Monitor time");
        System.out.println(monitorTime2);
    }
}