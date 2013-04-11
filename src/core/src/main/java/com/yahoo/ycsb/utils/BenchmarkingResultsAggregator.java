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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
public class BenchmarkingResultsAggregator {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
                                                                                           //\\p{Alpha}* => .*
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("([^\\d]*)-([\\d\\.]*)-(.*)\\.workload(?:-target-(\\d*))?(?:-(\\d*))?\\.txt");

    private static final String AGGREGATED_FILE_NAME = "%1$s-%2$s-%3$s.workload-aggregated.csv";

    private static final Pattern THROUGHPUT_PATTERN = Pattern.compile("\\[(\\w*)\\], (Throughput\\(ops/sec\\)), ([\\d\\.]*)");

    private static final Pattern AVERAGE_LATENCY_PATTERN = Pattern.compile("\\[(.*)\\], (AverageLatency\\(us\\)), ([\\d\\.]*)");

    private static final Pattern PERCENTILE_95th_LATENCY_PATTERN = Pattern.compile("\\[(.*)\\], (95thPercentileLatency\\(us\\)), ([\\d\\.]*)");

    private static final Pattern PERCENTILE_99th_LATENCY_PATTERN = Pattern.compile("\\[(.*)\\], (99thPercentileLatency\\(us\\)), ([\\d\\.]*)");

    private Map<String, Set<String>> writtenHeaders = new LinkedHashMap<String, Set<String>>();
    private File input;
    private File output;

    class BenchmarkingResultsInfo {
        private String database;
        private String version;
        private String workload;
        private Integer target;
        private Integer iteration;

        public BenchmarkingResultsInfo(String database, String version, String workload) {
            this.database = database;
            this.version = version;
            this.workload = workload;
        }

        public BenchmarkingResultsInfo(String database, String version, String workload, Integer target, Integer iteration) {
            this.database = database;
            this.version = version;
            this.workload = workload;
            this.target = target;
            this.iteration = iteration;
        }

        public String getDatabase() {
            return database;
        }

        public String getVersion() {
            return version;
        }

        public String getWorkload() {
            return workload;
        }

        public Integer getTarget() {
            return target;
        }

        public Integer getIteration() {
            return iteration;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            BenchmarkingResultsInfo that = (BenchmarkingResultsInfo) o;

            if (database != null ? !database.equals(that.database) : that.database != null) return false;
            if (target != null ? !target.equals(that.target) : that.target != null) return false;
            if (version != null ? !version.equals(that.version) : that.version != null) return false;
            if (workload != null ? !workload.equals(that.workload) : that.workload != null) return false;
            if (iteration != null ? !iteration.equals(that.iteration) : that.iteration != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = database != null ? database.hashCode() : 0;
            result = 31 * result + (version != null ? version.hashCode() : 0);
            result = 31 * result + (workload != null ? workload.hashCode() : 0);
            result = 31 * result + (target != null ? target.hashCode() : 0);
            result = 31 * result + (iteration != null ? iteration.hashCode() : 0);
            return result;
        }
    }

    public BenchmarkingResultsAggregator(String[] args) {
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
        System.out.println("Usage: com.yahoo.ycsb.utils.BenchmarkingResultsAggregator [input] [output]");
        System.out.println("[input]: a directory with benchmarking results");
        System.out.println("[output]: a directory to output");
    }

    private void aggregate() throws IOException {
        File[] files = input.listFiles(new FileFilter() {
            @Override
            public boolean accept(File path) {
                return path.isFile() && FILE_NAME_PATTERN.matcher(path.getName()).find();
            }
        });
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                Matcher matcher1 = FILE_NAME_PATTERN.matcher(file1.getName());
                Matcher matcher2 = FILE_NAME_PATTERN.matcher(file2.getName());
                if (!(matcher1.find() && matcher2.find())) {
                    return 0;
                }
                int comparison = matcher1.group(1).compareTo(matcher2.group(1));
                if (comparison != 0) {
                    return comparison;
                }
                comparison = String.valueOf(matcher1.group(2)).compareTo(String.valueOf(matcher2.group(2)));
                if (comparison != 0) {
                    return comparison;
                }
                comparison = matcher1.group(3).compareTo(matcher2.group(3));
                if (comparison != 0) {
                    return comparison;
                }
                Integer target1 = matcher1.group(4) != null ? Integer.parseInt(matcher1.group(4)) : null;
                Integer target2 = matcher2.group(4) != null ? Integer.parseInt(matcher2.group(4)) : null;
                return target1 != null && target2 != null ? target1.compareTo(target2) : 0;
            }
        });
        Map<BenchmarkingResultsInfo, OutputStream> outputStreams = new HashMap<BenchmarkingResultsInfo, OutputStream>();
        for (File file : files) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
            if (!matcher.find()) {
                continue;
            }
            BenchmarkingResultsInfo groupInfo = new BenchmarkingResultsInfo(
                    matcher.group(1), matcher.group(2), matcher.group(3));
            OutputStream outputStream = outputStreams.get(groupInfo);
            if (outputStream == null) {
                String outputFile = String.format(AGGREGATED_FILE_NAME,
                        groupInfo.getDatabase(), groupInfo.getVersion(), groupInfo.getWorkload());
                outputStream = new FileOutputStream(new File(output, outputFile));
                outputStreams.put(groupInfo, outputStream);
            }
            String target = matcher.group(4);
            String iteration = matcher.group(5);
            BenchmarkingResultsInfo info = new BenchmarkingResultsInfo(
                    groupInfo.getDatabase(), groupInfo.getVersion(),
                    groupInfo.getWorkload(),
                    target != null ? Integer.parseInt(target) : null,
                    iteration != null ? Integer.parseInt(iteration) : null);
            FileInputStream inputStream = new FileInputStream(file);
            process(info, inputStream, outputStream);
        }
        for (OutputStream outputStream : outputStreams.values()) {
            outputStream.flush();
            outputStream.close();
        }
    }

    private static String getCommaSeparatedValues(Collection<String> values) {
        StringBuilder line = new StringBuilder();
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext(); ) {
            line.append(iterator.next());
            if (iterator.hasNext()) {
                line.append(",");
            }
        }
        return line.toString();
    }

    private static String toCamelCase(String text) {
        StringTokenizer tokens = new StringTokenizer(text, "-");
        StringBuilder value = new StringBuilder();
        while (tokens.hasMoreTokens()) {
            String token = tokens.nextToken();
            value.append(token.substring(0, 1).toUpperCase());
            value.append(token.substring(1).toLowerCase());
        }
        return value.toString();
    }

    private void process(BenchmarkingResultsInfo info, InputStream input, OutputStream output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(output));

        String group = String.format("%1$s-%2$s-%3$s", info.getDatabase(), info.getVersion(), info.getWorkload());
        Set<String> headers = writtenHeaders.get(group);
        boolean writeHeaders;
        if (writeHeaders = (headers == null)) {
            writtenHeaders.put(group, headers = new LinkedHashSet<String>());
        }
        String line;
        List<String> values = new ArrayList<String>();
        while ((line = reader.readLine()) != null) {
            for (Matcher matcher : new Matcher[] {
                    THROUGHPUT_PATTERN.matcher(line),
                    AVERAGE_LATENCY_PATTERN.matcher(line),
                    PERCENTILE_95th_LATENCY_PATTERN.matcher(line),
                    PERCENTILE_99th_LATENCY_PATTERN.matcher(line)}) {
                if (!matcher.find()) {
                    continue;
                }
                String header = toCamelCase(matcher.group(1)) + matcher.group(2);
                headers.add(header);
                values.add(matcher.group(3));
            }
        }
        if (writeHeaders) {
            output.write(getCommaSeparatedValues(headers).getBytes());
            output.write(LINE_SEPARATOR.getBytes());
        }
        int lines = values.size() / headers.size();
        int perLine = headers.size();
        for (int i = 0; i < lines; i++) {
            String valuesLine = getCommaSeparatedValues(values.subList(perLine * i, perLine * (i + 1)));
            output.write(valuesLine.getBytes());
            output.write(LINE_SEPARATOR.getBytes());
        }
        reader.close();
        writer.flush();
    }

    public static void main(String[] args) throws Exception {
        List<String> inputs = Arrays.asList(
                "/Users/tazija/Downloads/ycsb/mongodb-2.0.6-write-replicas-safe-read-secondary-journaling",
                "/Users/tazija/Downloads/ycsb/mongodb-2.0.6-write-replicas-safe-read-secondary-nojournaling",
                "/Users/tazija/Downloads/ycsb/mongodb-2.2.0-rc0-write-replicas-safe-read-secondary-nojournaling",
                "/Users/tazija/Downloads/ycsb/mongodb-2.2.0-rc0-write-replicas-safe-read-secondary-nojournaling-2kb",
                "/Users/tazija/Downloads/ycsb/couchbase-2.0.0-replication-read-buffer-16384"
        );
        for (String input : inputs) {
            String output = input + "-aggregated";
            BenchmarkingResultsAggregator aggregator = new BenchmarkingResultsAggregator(
                    new String[]{input, output}
            );
            aggregator.aggregate();
        }
    }
}
