/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CalculateAverage_obourgain {

    private static final String FILE = "./measurements.txt";

    private static final ThreadLocal<BitTwiddledMap> THREAD_LOCAL_RESULTS_MAP = ThreadLocal.withInitial(BitTwiddledMap::new);

    private static class MeasurementAggregator {
        // deci-Celcius values
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private long sum;
        private int count;

        public String toString() {
            return STR."\{round(min)}/\{round(sum / count)}/\{round(max)}";
        }

        private double round(double value) {
            // here I have a difference with the reference implementation because it goes straight to using double for the sum, and I keep the full precision, and end up having slightly different results once
            // rounded
            // division by 100 because we kept the values in deci-Celcius. I would argue that my implementation is more precise, but sometimes the goal is being bug for bug compatible
            return Math.round(value * 10.0) / 100.0;
        }
    }

    public static void main(String[] args) throws Exception {
        // no close, leak everything and let the OS cleanup!
        var randomAccessFile = new RandomAccessFile(FILE, "r");
        MemorySegment segment = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length(), Arena.global());

        int cpus = Runtime.getRuntime().availableProcessors();
        // can we do better to balance across cpu cores?
        int chunkSize = 20 * 1024 * 1024;
        List<MemorySegment> chunks = createChunks(segment, chunkSize);

        var mergedResults = new ConcurrentHashMap<String, MeasurementAggregator>();
        try (ThreadPoolExecutor executor = new ThreadPoolExecutor(cpus, cpus, 1L, TimeUnit.DAYS, new LinkedBlockingQueue<>(), r -> {
            var thread = new Thread(r);
            thread.setUncaughtExceptionHandler((t, e) -> {
                e.printStackTrace();
            });
            return thread;
        })) {
            chunks.forEach(c -> executor.execute(() -> {
                BitTwiddledMap resultMap = processChunk(c);
                merge(mergedResults, resultMap);
            }));
            executor.shutdown();
            boolean shutdownProperly = executor.awaitTermination(1, TimeUnit.MINUTES);
            if (!shutdownProperly) {
                throw new RuntimeException("did not complete on time");
            }
        }
        var finalResult = new TreeMap<>(mergedResults);
        System.out.println(finalResult);
    }

    static class PerCityStats {
        // all values are in deci-Celcius
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        long sum;
        int count;

        public void reset() {
            min = Integer.MAX_VALUE;
            max = Integer.MIN_VALUE;
            sum = 0;
            count = 0;
        }

        void add(int measurementInDeciCelsius) {
            min = Math.min(min, measurementInDeciCelsius);
            max = Math.max(max, measurementInDeciCelsius);
            sum += measurementInDeciCelsius;
            count++;
        }
    }

    private static BitTwiddledMap processChunk(MemorySegment segment) {
        var result = THREAD_LOCAL_RESULTS_MAP.get();
        // let's assume that's enough
        // https://en.wikipedia.org/wiki/List_of_long_place_names
        var cityNameBuffer = new byte[128];

        int size = Math.toIntExact(segment.byteSize());
        long position = 0;

        var hashCodeHolder = new int[1];

        while (position < size - 1) {
            position = processLineInChunk(segment, position, cityNameBuffer, hashCodeHolder, result);
        }
        return result;
    }

    private static long processLineInChunk(MemorySegment segment, long position, byte[] cityNameBuffer, int[] hashCode, BitTwiddledMap result) {
        // compute hashCode for the city name, copy the bytes to a buffer and search for the semicolon all at once, so we don' t visit the same byte twice
        int cityNameLength = getCityNameLength(segment, position, cityNameBuffer, hashCode);
        position += cityNameLength + 1;
        long cityNameEnd = position - 1;
        PerCityStats perCityStats = result.getOrCreate(cityNameBuffer, cityNameLength, hashCode[0]);

        // keep in deci-Celcius, so we avoid a division for each line, and keep it for the end
        int valueLength = hackyDecodeDouble(segment, cityNameEnd + 1, perCityStats);

        long lineEnd = position + valueLength;
        // +1 to skip the line feed
        position = lineEnd + 1;
        return position;
    }

    private static int getCityNameLength(MemorySegment segment, long position, byte[] cityNameBuffer, int[] hashCodeHolder) {
        int cityNameLength = 0;
        int cityNameHashCode = 0;
        byte b;
        long pos = position;
        while ((b = segment.get(ValueLayout.JAVA_BYTE, pos++)) != ';') {
            cityNameHashCode = cityNameHashCode * 31 + b;
            cityNameBuffer[cityNameLength] = b;
            cityNameLength++;
        }
        hashCodeHolder[0] = cityNameHashCode;
        return cityNameLength;
    }

    private static int hackyDecodeDouble(MemorySegment segment, long valueStart, PerCityStats perCityStats) {
        // values are assumed to be:
        // * maybe with a minus sign
        // * an integer part in the range of 0 to 99 included, single digit possible
        // * always with a single decimal
        int negative = 1;
        int offset = 0;
        byte maybeSign = segment.get(ValueLayout.JAVA_BYTE, valueStart);
        if (maybeSign == '-') {
            offset++;
            negative = -1;
        }

        byte firstDigit = segment.get(ValueLayout.JAVA_BYTE, valueStart + offset);
        offset++;
        int tempInDeciCelcius = firstDigit - '0';
        byte secondDigitOrDor = segment.get(ValueLayout.JAVA_BYTE, valueStart + offset);
        if (secondDigitOrDor != '.') {
            offset++;
            tempInDeciCelcius = 10 * tempInDeciCelcius + (secondDigitOrDor - '0');
        }
        offset++;
        byte decimalDigit = segment.get(ValueLayout.JAVA_BYTE, valueStart + offset);
        tempInDeciCelcius = 10 * tempInDeciCelcius + (decimalDigit - '0');
        tempInDeciCelcius *= negative;
        perCityStats.add(tempInDeciCelcius);
        offset++;
        return offset;
    }

    private static void merge(ConcurrentHashMap<String, MeasurementAggregator> mergedResults, BitTwiddledMap chunkResult) {
        chunkResult.values.forEach(e -> {
            String keyAsString = new String(e.key);
            // compute is atomic, so we don't need to synchronize
            mergedResults.compute(keyAsString, (_, v) -> {
                if (v == null) {
                    v = new MeasurementAggregator();
                }
                v.min = Math.min(v.min, e.measurement.min);
                v.max = Math.max(v.max, e.measurement.max);
                v.sum += e.measurement.sum;
                v.count += e.measurement.count;
                return v;
            });
            e.measurement.reset();
        });
    }

    static List<MemorySegment> createChunks(MemorySegment segment, int chunkSize) {
        var result = new ArrayList<MemorySegment>();

        long endOfPreviousChunk = 0;
        while (endOfPreviousChunk < segment.byteSize()) {
            long chunkStart = endOfPreviousChunk;

            long tmpChunkEnd = Math.min(segment.byteSize() - 1, endOfPreviousChunk + chunkSize);
            long chunkEnd;
            if (segment.get(ValueLayout.JAVA_BYTE, tmpChunkEnd) == '\n') {
                // we got lucky and our chunk ends on a line break
                chunkEnd = tmpChunkEnd + 1;
            }
            else {
                // round the chunk to the next line break, included
                chunkEnd = findNextLineBreak(segment, tmpChunkEnd) + 1;
            }
            MemorySegment slice = segment.asSlice(chunkStart, chunkEnd - chunkStart);
            result.add(slice);
            endOfPreviousChunk = chunkEnd;
        }
        return result;
    }

    static long findNextLineBreak(MemorySegment segment, long start) {

        long limit = segment.byteSize();
        for (long i = start; i < limit; i++) {
            byte b = segment.get(ValueLayout.JAVA_BYTE, i);
            if (b == '\n') {
                return i;
            }
        }
        return segment.byteSize();
    }

    // I shamelessly stole the impl from Roy Van Rijn, thanks! Modified to suit my needs

    /**
     * A normal Java HashMap does all these safety things like boundary checks... we don't need that, we need speeeed.
     * <p>
     * So I've written an extremely simple linear probing hashmap that should work well enough.
     */
    static class BitTwiddledMap {
        private static final int SIZE = 16384; // A bit larger than the number of keys, needs power of two
        private final int[] indices = new int[SIZE]; // Hashtable is just an int[]

        BitTwiddledMap() {
            // Optimized fill with -1, fastest method:
            int len = indices.length;
            if (len > 0) {
                indices[0] = -1;
            }
            // Value of i will be [1, 2, 4, 8, 16, 32, ..., len]
            for (int i = 1; i < len; i += i) {
                System.arraycopy(indices, 0, indices, i, i);
            }
        }

        private final List<BitTwiddledMap.Entry> values = new ArrayList<>(512);

        record Entry(int hash, byte[] key, PerCityStats measurement) {
            @Override
            public String toString() {
                return new String(key) + "=" + measurement;
            }
        }

        /**
         * Who needs methods like add(), merge(), compute() etc, we need one, getOrCreate.
         *
         * @param key
         * @return
         */
        public PerCityStats getOrCreate(byte[] key, int length, int hashCode) {
            int index = (SIZE - 1) & hashCode;
            int valueIndex;
            BitTwiddledMap.Entry retrievedEntry = null;
            while ((valueIndex = indices[index]) != -1 && (retrievedEntry = values.get(valueIndex)).hash != hashCode) {
                index = (index + 1) % SIZE;
            }
            if (valueIndex >= 0) {
                return retrievedEntry.measurement;
            }
            // New entry, insert into table and return.
            indices[index] = values.size();

            // Only parse this once:
            byte[] actualKey = new byte[length];
            System.arraycopy(key, 0, actualKey, 0, length);

            BitTwiddledMap.Entry toAdd = new BitTwiddledMap.Entry(hashCode, actualKey, new PerCityStats());
            values.add(toAdd);
            return toAdd.measurement;
        }
    }
}
