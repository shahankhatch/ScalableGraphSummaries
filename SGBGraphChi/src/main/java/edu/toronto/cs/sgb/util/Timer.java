package edu.toronto.cs.sgb.util;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Timer {

    private static final DecimalFormat df = new DecimalFormat("#.###");

    private final String label;
    private long start;
    private long end;
    boolean flagRunning = false;

    public Timer(String label, boolean startImmediately) {
        this(label);
        if (startImmediately) {
            this.start();
        }
    }

    public Timer(String label) {
        this.label = label;
    }

    public String start() {
        flagRunning = true;
        start = System.currentTimeMillis();
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(start);
        return getMessageHeader() + " STARTING";
    }

    private String getMessageHeader() {
        return "[Timer], " + label + ",";
    }

    public Timer stop() {
        flagRunning = false;
        end = System.currentTimeMillis();
        return this;
    }

    public double getTime() {
        if (flagRunning) {
            return System.currentTimeMillis() - start;
        }
        return end - start;
    }

    @Override
    public String toString() {
        double time = getTime();
        double times = getTimeS();
        double timem = getTimeM();
        double timeh = getTimeH();
        String running = flagRunning ? "running" : "stopped";
        return getMessageHeader() + " " + running + " (h m s ms), " + df.format(timeh) + ", " + df.format(timem) + ", " + df.format(times) + ", " + time;
    }

    public double getTimeH() {
        double timem = getTimeM();
        return timem / 60;
    }

    public double getTimeM() {
        double times = getTimeS();
        return times / 60;
    }

    public double getTimeS() {
        double time = getTime();
        return time / 1000;
    }
}
