package DZ3;
import java.awt.Color;
import java.util.concurrent.locks.ReentrantLock;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public class Mandelbrot {
    private static final int WIDTH = 1920;
    private static final int HEIGHT = 1200;
    private static final int MAX_ITERATION=500;
    private static final int THREADS =
            Runtime.getRuntime().availableProcessors();
    private static final double MIN_REAL=-2.0;
    private static final double MAX_REAL=1.0;
    private static final double MIN_IMAGINARY=-1.2;
    private static final double MAX_IMAGINARY=1.2;
    private static final int[] pixels =
            new int[WIDTH * HEIGHT];
    private static int calculateIterations(double cReal, double cImaginary) {
        double zReal=0.0;
        double zImaginary=0.0;
        int iteration =0;
        while (zReal *zReal + zImaginary *zImaginary <=4.0 &&
                iteration<MAX_ITERATION) {
            double nextReal=zReal*zReal - zImaginary*zImaginary + cReal;
            double nextImaginary=2.0*zReal*zImaginary + cImaginary;
            zReal=nextReal;
            zImaginary=nextImaginary;
            iteration++;
        }
        return iteration;
    }
    private static int calculateColor(int iteraions){
        if (iteraions==MAX_ITERATION){
            return Color.BLACK.getRGB();
        }
        float hue=
                0.7f-0.7f*iteraions/MAX_ITERATION;
        return Color.HSBtoRGB(
                hue, 1.0f, 1.0f
        );
    }
    public static void calculateRows(
            int StartY,
            int endY
    ) {
        for (int pixelY=StartY;
        pixelY<endY;
        pixelY++){
            for (int pixelX=0;
            pixelX<WIDTH;
            pixelX++){
                double cReal=MIN_REAL+pixelX*(MAX_REAL-MIN_REAL)/(WIDTH-1);
                double cImaginary= MAX_IMAGINARY-pixelY*(MAX_IMAGINARY-MIN_IMAGINARY)/(HEIGHT-1);
            int iteration = calculateIterations(cReal, cImaginary);
            int color=calculateColor(iteration);
            int index=pixelY*WIDTH+pixelX;
            pixels[index]=color;
            }
        }
    }
    private static Thread createThread(int threadNumber){
        return new Thread(() -> {
            int startY = threadNumber * HEIGHT / THREADS;
            int endY = (threadNumber + 1) * HEIGHT / THREADS;
            System.out.println(Thread.currentThread().getName() + ":строки" + startY + '-' + (endY - 1));
            calculateRows(startY, endY);
        },"Поток"+threadNumber);
    }
    private static void calculateParallel()
        throws  InterruptedException{
        Thread[] threads = new Thread[THREADS];
        for (int i = 0; i < THREADS; i++) {
            threads[i] = createThread(i);
        }
        for(Thread thread:threads){
            thread.start();
        }
        for(Thread thread:threads){
            thread.join();
        }
        }
    private static void saveImage(String fileName)
            throws IOException {

        BufferedImage image = new BufferedImage(
                WIDTH,
                HEIGHT,
                BufferedImage.TYPE_INT_RGB
        );

        image.setRGB(
                0,
                0,
                WIDTH,
                HEIGHT,
                pixels,
                0,
                WIDTH
        );

        ImageIO.write(
                image,
                "png",
                new File(fileName)
        );
    }
    public static void main(String[] args)
            throws InterruptedException, IOException {

        System.out.println(
                "Количество потоков: " + THREADS
        );

        long startTime = System.nanoTime();

        calculateParallel();

        long finishTime = System.nanoTime();

        double timeMilliseconds =
                (finishTime - startTime) / 1_000_000.0;

        System.out.printf(
                "Время вычисления: %.3f мс%n",
                timeMilliseconds
        );

        String fileName = "mandelbrot.png";

        saveImage(fileName);

        System.out.println(
                "Изображение сохранено: "
                        + new File(fileName).getAbsolutePath()
        );
    }
}
