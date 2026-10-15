package DZ5;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LogAnalysisAssignment {
    public record LogEntry(String method, String path, int status,
                           long responseBytes, long durationMillis) {}

    public record LogFile(Path path) {}

    public record PathStatistics(long requests, long successful, long errors,
                                 long responseBytes, long totalDurationMillis,
                                 long maxDurationMillis) {}

    public record Report(long totalRequests, long successfulRequests,
                         long errorRequests, long responseBytes,
                         long maxDurationMillis,
                         Map<String, PathStatistics> paths,
                         long processedFiles, long readLines, long validRecords,
                         long invalidRecords, long readErrors,
                         int activeFiles, int maxActiveFiles,
                         List<String> failedFiles, boolean stopped) {}

    public static LogEntry parseLine(String line) {
        if (line == null || line.isBlank()) {
            throw new IllegalArgumentException("Пустая строка лога");
        }
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException("Ожидалось 5 полей: " + line);
        }
        try {
            int status = Integer.parseInt(fields[2]);
            long bytes = Long.parseLong(fields[3]);
            long duration = Long.parseLong(fields[4]);
            if (status < 200 || status > 599) {
                throw new IllegalArgumentException("Недопустимый статус: " + status);
            }
            if (bytes < 0 || duration < 0) {
                throw new IllegalArgumentException("Объём и время не могут быть отрицательными");
            }
            return new LogEntry(fields[0], fields[1], status, bytes, duration);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Некорректное число: " + line, e);
        }
    }

    public static class LogFileLines implements Iterator<String>, AutoCloseable {
        private final BufferedReader reader;
        private String nextLine;
        private boolean loaded;

        private LogFileLines(Path path) throws IOException {
            reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
        }

        public static LogFileLines iterator(Path path) throws IOException {
            return new LogFileLines(path);
        }

        public boolean hasNext() {
            if (!loaded) {
                try {
                    nextLine = reader.readLine();
                    loaded = true;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return nextLine != null;
        }

        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            String line = nextLine;
            loaded = false;
            return line;
        }

        public void close() throws IOException {
            reader.close();
        }
    }

    public static class PathAccumulator {
        private long requests;
        private long successful;
        private long errors;
        private long bytes;
        private long totalDuration;
        private long maxDuration;

        public synchronized void add(LogEntry entry) {
            requests++;
            if (entry.status() < 400) {
                successful++;
            } else {
                errors++;
            }
            bytes += entry.responseBytes();
            totalDuration += entry.durationMillis();
            maxDuration = Math.max(maxDuration, entry.durationMillis());
        }

        public synchronized PathStatistics getStatistics() {
            return new PathStatistics(requests, successful, errors, bytes,
                    totalDuration, maxDuration);
        }
    }

    public static class Statistics {
        private final boolean globalLock;
        private final Map<String, PathAccumulator> paths;

        public Statistics(boolean globalLock) {
            this.globalLock = globalLock;
            if (globalLock) {
                paths = new HashMap<>();
            } else {
                paths = new ConcurrentHashMap<>();
            }
        }

        public void add(LogEntry entry) {
            if (globalLock) {
                addWithGlobalLock(entry);
            } else {
                paths.computeIfAbsent(entry.path(), key -> new PathAccumulator()).add(entry);
            }
        }

        private synchronized void addWithGlobalLock(LogEntry entry) {
            paths.computeIfAbsent(entry.path(), key -> new PathAccumulator()).add(entry);
        }

        public Map<String, PathStatistics> getStatistics() {
            Map<String, PathStatistics> result = new HashMap<>();
            for (var entry : paths.entrySet()) {
                result.put(entry.getKey(), entry.getValue().getStatistics());
            }
            return Map.copyOf(result);
        }
    }

    public static class Analyzer {
        private volatile boolean stopped;
        private final Statistics statistics;
        private final Semaphore semaphore;
        private final AtomicLong processedFiles = new AtomicLong();
        private final AtomicLong readLines = new AtomicLong();
        private final AtomicLong validRecords = new AtomicLong();
        private final AtomicLong invalidRecords = new AtomicLong();
        private final AtomicLong readErrors = new AtomicLong();
        private final AtomicInteger activeFiles = new AtomicInteger();
        private final AtomicInteger maxActiveFiles = new AtomicInteger();
        private final ConcurrentLinkedQueue<String> failedFiles = new ConcurrentLinkedQueue<>();

        public Analyzer(int fileLimit, boolean globalLock) {
            if (fileLimit < 1) {
                throw new IllegalArgumentException("Лимит файлов должен быть положительным");
            }
            semaphore = new Semaphore(fileLimit);
            statistics = new Statistics(globalLock);
        }

        public void stop() {
            stopped = true;
        }

        private void updateMaximum(int active) {
            int old = maxActiveFiles.get();
            while (active > old) {
                if (maxActiveFiles.compareAndSet(old, active)) {
                    break;
                }
                old = maxActiveFiles.get();
            }
        }

        private void processFile(LogFile file) {
            boolean acquired = false;
            try {
                if (stopped) {
                    return;
                }
                semaphore.acquire();
                acquired = true;
                updateMaximum(activeFiles.incrementAndGet());
                if (stopped) {
                    return;
                }
                boolean finished = false;
                try (LogFileLines lines = LogFileLines.iterator(file.path())) {
                    while (!stopped && !Thread.currentThread().isInterrupted()) {
                        if (!lines.hasNext()) {
                            finished = true;
                            break;
                        }
                        String line = lines.next();
                        readLines.incrementAndGet();
                        try {
                            LogEntry entry = parseLine(line);
                            statistics.add(entry);
                            validRecords.incrementAndGet();
                        } catch (IllegalArgumentException e) {
                            invalidRecords.incrementAndGet();
                        }
                    }
                }
                if (finished) {
                    processedFiles.incrementAndGet();
                }
            } catch (InterruptedException e) {
                stop();
                Thread.currentThread().interrupt();
            } catch (IOException | UncheckedIOException | SecurityException e) {
                readErrors.incrementAndGet();
                failedFiles.add(file.path() + ": " + e.getMessage());
            } finally {
                if (acquired) {
                    activeFiles.decrementAndGet();
                    semaphore.release();
                }
            }
        }

        public Report sequential(List<LogFile> files) {
            for (LogFile file : files) {
                processFile(file);
            }
            return buildReport();
        }

        public Report parallel(List<LogFile> files, int threads) throws InterruptedException {
            if (threads < 1) {
                throw new IllegalArgumentException("Число потоков должно быть положительным");
            }
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(files.size());
            boolean interrupted = false;
            try {
                for (LogFile file : files) {
                    pool.submit(() -> {
                        try {
                            processFile(file);
                        } finally {
                            latch.countDown();
                        }
                    });
                }
                latch.await();
            } catch (InterruptedException e) {
                stop();
                interrupted = true;
                pool.shutdownNow();
            } finally {
                pool.shutdown();
                while (!pool.isTerminated()) {
                    try {
                        pool.awaitTermination(1, TimeUnit.DAYS);
                    } catch (InterruptedException e) {
                        stop();
                        interrupted = true;
                        pool.shutdownNow();
                    }
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
                throw new InterruptedException("Анализ прерван");
            }
            return buildReport();
        }

        private Report buildReport() {
            Map<String, PathStatistics> paths = statistics.getStatistics();
            long requests = 0;
            long successful = 0;
            long errors = 0;
            long bytes = 0;
            long maxDuration = 0;
            for (PathStatistics value : paths.values()) {
                requests += value.requests();
                successful += value.successful();
                errors += value.errors();
                bytes += value.responseBytes();
                maxDuration = Math.max(maxDuration, value.maxDurationMillis());
            }
            return new Report(requests, successful, errors, bytes, maxDuration, paths,
                    processedFiles.get(), readLines.get(), validRecords.get(),
                    invalidRecords.get(), readErrors.get(), activeFiles.get(),
                    maxActiveFiles.get(), List.copyOf(failedFiles), stopped);
        }
    }

    public static class GenerateLogs {
        private static final String[] METHODS = {"GET", "POST", "PUT", "DELETE"};
        private static final String[] PATHS = {"/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"};
        private static final int[] STATUSES = {200, 200, 200, 201, 204, 301, 400, 401, 404, 500};

        public static void generate(Path directory, int fileCount, int linesPerFile) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException("Количество не может быть отрицательным");
            }
            Files.createDirectories(directory);
            Random random = new Random(42);
            for (int i = 0; i < fileCount; i++) {
                Path file = directory.resolve("access-%03d.log".formatted(i));
                try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
                    for (int j = 0; j < linesPerFile; j++) {
                        writer.write(METHODS[random.nextInt(METHODS.length)] + " "
                                + PATHS[random.nextInt(PATHS.length)] + " "
                                + STATUSES[random.nextInt(STATUSES.length)] + " "
                                + (100 + random.nextInt(100000)) + " "
                                + (1 + random.nextInt(500)));
                        writer.newLine();
                    }
                }
            }
        }
    }

    private static List<LogFile> findFiles(Path directory) throws IOException {
        List<LogFile> result = new ArrayList<>();
        try (var paths = Files.list(directory)) {
            for (Path path : paths.sorted().toList()) {
                if (path.getFileName().toString().endsWith(".log")) {
                    result.add(new LogFile(path));
                }
            }
        }
        return result;
    }

    private static void printReport(Report report) {
        for (String path : report.paths().keySet().stream().sorted().toList()) {
            System.out.println(path + " " + report.paths().get(path));
        }
        System.out.println("Всего запросов: " + report.totalRequests());
        System.out.println("Успешных: " + report.successfulRequests());
        System.out.println("Ошибочных: " + report.errorRequests());
        System.out.println("Объём ответов: " + report.responseBytes());
        System.out.println("Максимальное время: " + report.maxDurationMillis());
        System.out.println("Полностью обработано файлов: " + report.processedFiles());
        System.out.println("Прочитано строк: " + report.readLines());
        System.out.println("Корректных записей: " + report.validRecords());
        System.out.println("Повреждённых записей: " + report.invalidRecords());
        System.out.println("Ошибок чтения: " + report.readErrors());
        System.out.println("Активных файлов: " + report.activeFiles());
        System.out.println("Максимум активных файлов: " + report.maxActiveFiles());
        System.out.println("Остановлен: " + report.stopped());
        for (String file : report.failedFiles()) {
            System.out.println("Не удалось обработать: " + file);
        }
    }

    private static void selfCheck() throws Exception {
        Path directory = Files.createTempDirectory("log-assignment-");
        try {
            GenerateLogs.generate(directory, 3, 7);
            List<LogFile> files = findFiles(directory);
            Report sequential = new Analyzer(1, true).sequential(files);
            for (boolean global : new boolean[]{true, false}) {
                for (int threads : new int[]{1, 4}) {
                    for (int limit : new int[]{1, 4}) {
                        Report parallel = new Analyzer(limit, global).parallel(files, threads);
                        if (!sequential.paths().equals(parallel.paths())
                                || parallel.totalRequests() != 21
                                || parallel.processedFiles() != 3
                                || parallel.readLines() != 21
                                || parallel.validRecords() != 21
                                || parallel.invalidRecords() != 0
                                || parallel.readErrors() != 0
                                || parallel.activeFiles() != 0
                                || parallel.maxActiveFiles() > limit) {
                            throw new AssertionError("Ошибка параллельного отчёта");
                        }
                    }
                }
            }
            Files.writeString(directory.resolve("broken.log"), "broken\nGET /test 200 10 5\n");
            files = findFiles(directory);
            files.add(new LogFile(directory.resolve("missing.log")));
            Report partial = new Analyzer(2, false).parallel(files, 4);
            if (partial.totalRequests() != 22 || partial.invalidRecords() != 1
                    || partial.readLines() != 23 || partial.processedFiles() != 4
                    || partial.readErrors() != 1 || partial.failedFiles().size() != 1
                    || partial.activeFiles() != 0) {
                throw new AssertionError("Ошибка частичного отчёта");
            }
            Analyzer analyzer = new Analyzer(1, false);
            analyzer.stop();
            Report stopped = analyzer.parallel(files, 4);
            if (!stopped.stopped() || stopped.readLines() != 0 || stopped.activeFiles() != 0) {
                throw new AssertionError("Ошибка остановки");
            }
            System.out.println("OK: последовательный отчёт, потоки, ошибки и остановка");
        } finally {
            try (var paths = Files.list(directory)) {
                for (Path path : paths.toList()) {
                    Files.delete(path);
                }
            }
            Files.delete(directory);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            int files = args.length > 2 ? Integer.parseInt(args[2]) : 20;
            int lines = args.length > 3 ? Integer.parseInt(args[3]) : 1000;
            GenerateLogs.generate(directory, files, lines);
            System.out.println("Создано файлов: " + files);
            return;
        }
        if (args.length > 0 && (args[0].equals("sequential") || args[0].equals("parallel"))) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            List<LogFile> files = findFiles(directory);
            int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
            int limit = args.length > 3 ? Integer.parseInt(args[3]) : 2;
            boolean global = args.length > 4 && args[4].equals("global");
            Analyzer analyzer = new Analyzer(limit, global);
            long start = System.nanoTime();
            Report report;
            if (args[0].equals("sequential")) {
                report = analyzer.sequential(files);
            } else {
                report = analyzer.parallel(files, threads);
            }
            long time = System.nanoTime() - start;
            printReport(report);
            System.out.println("Время анализа, мс: " + time / 1000000.0);
            return;
        }
        System.out.println("generate <каталог> <файлы> <строки>");
        System.out.println("sequential <каталог>");
        System.out.println("parallel <каталог> <потоки> <лимит> <global или local>");
        System.out.println("check");
    }
}
/*
Я сравнил последовательную и параллельную обработку 20 файлов по 1000 строк.
В обоих случаях обработано 20000 запросов:
12041 успешный и 7959 ошибочных.
Статистика по всем URL совпала.

Последовательная обработка заняла 123,55 мс, параллельная — 109,77 мс.
В этом запуске параллельная версия была примерно на 11% быстрее.

Одновременно читалось не больше двух файлов, как задано в настройках.
После завершения активных файлов осталось 0.
Ошибок чтения и повреждённых строк не было.
*/
