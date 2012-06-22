package com.yahoo.ycsb.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class BenchmarkingResultsCleaner {

    private static final String START_STRING = "YCSB Client";

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private File input;
    private File output;

    public BenchmarkingResultsCleaner(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(0);
        }
        this.input = new File(args[0]);
        this.output = new File(args[1]);
        if (!this.input.isDirectory()) {
            usage();
            System.exit(0);
        }
        this.output.mkdirs();
        if (!this.output.isDirectory()) {
            usage();
            System.exit(0);
        }
    }

    private void usage() {
        System.out.println("Usage: com.yahoo.ycsb.utils.BenchmarkingResultsCleaner [input] [output]");
        System.out.println("[input]: a directory with benchmarking results");
        System.out.println("[output]: a directory to output");
    }

    private void process(InputStream input, OutputStream output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output));
        String line;
        boolean startStringFound = false;

        while ((line = reader.readLine()) != null) {
            if (!startStringFound) {
                startStringFound = line.startsWith(START_STRING);
            }
            if (startStringFound) {
                writer.write(line);
                writer.write(LINE_SEPARATOR);
            }
        }
        reader.close();

        writer.flush();
        writer.close();
    }

    private void clean() throws IOException {
        File []files = input.listFiles(new FileFilter() {
            @Override
            public boolean accept(File path) {
                return path.isFile();
            }
        });
        for (File file : files) {
            FileInputStream inputFile = new FileInputStream(file);
            FileOutputStream outputFile = new FileOutputStream(new File(output, file.getName()));
            process(inputFile, outputFile);
        }
    }

    public static void main(String[] args) throws Exception {
        BenchmarkingResultsCleaner cleaner;
        args = new String[]{
                "/Users/tazija/Downloads/ycsb/mongodb/20-threads-raid",
                "/Users/tazija/Downloads/ycsb/mongodb/20-threads-raid-cleaned"};
        cleaner = new BenchmarkingResultsCleaner(args);
        cleaner.clean();
    }

}
