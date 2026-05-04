/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.spanner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for ReadFileShardFn class. */
public final class ReadFileShardFnTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ReadFileShardFnTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static String testTableName = "TestTable";

  private final ValueProvider<Character> columnDelimiter = StaticValueProvider.of(',');
  private final ValueProvider<Character> fieldQualifier = StaticValueProvider.of('"');
  private final ValueProvider<Boolean> trailingDelimiter = StaticValueProvider.of(false);
  private final ValueProvider<Character> escapeChar = StaticValueProvider.of(null);
  private final ValueProvider<String> nullString = StaticValueProvider.of(null);
  private final ValueProvider<Boolean> handleNewLine = StaticValueProvider.of(true);

  @Test
  public void readFileBasicTest() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(0L, 30L), 3L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse(
                "1,abc,def\n2,abc,def\n3,abc,def\n", CSVFormat.newFormat(columnDelimiter.get()))
            .getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 3)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  @Test
  public void readMiddleShard() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(10L, 30L), 2L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse("2,abc,def\n3,abc,def\n", CSVFormat.newFormat(columnDelimiter.get()))
            .getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 2)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  @Test
  public void readLastShardWithoutNewline() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(30L, 39L), 1L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse("4,abc,def", CSVFormat.newFormat(columnDelimiter.get())).getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 1)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  @Test
  public void readLastShardWithNewline() throws Exception {
    Path inputFile = Files.createTempFile(testTableName, ".csv");

    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, charset)) {
      String data = "1,abc,def\n2,abc,def\n3,abc,def\n4,abc,def\n";
      writer.write(data, 0, data.length());
    } catch (IOException e) {
      e.printStackTrace();
    }
    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {

                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(30L, 40L), 1L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));
    // We convert the CSVRecord to a string containing the values as PAssert cannot compare equality
    // for CSVRecord directly.
    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));
    List<CSVRecord> csvRecords =
        CSVParser.parse("4,abc,def\n", CSVFormat.newFormat(columnDelimiter.get())).getRecords();

    List<KV<String, String>> expectedRecords =
        IntStream.range(0, 1)
            .mapToObj(i -> KV.of(testTableName, csvRecordToValues(csvRecords.get(i))))
            .collect(Collectors.toList());

    PAssert.that(csvValues).containsInAnyOrder(expectedRecords);

    pipeline.run();
  }

  @Test
  public void readNonAsciiFileEndToEnd() throws Exception {
    // Byte layout (UTF-8):
    //   "1,café\n"   = 8 bytes  (7 chars — 'é' encodes as 2 bytes: 0xC3 0xA9)
    //   "2,résumé\n" = 11 bytes (9 chars — two 'é' characters)
    //   "3,naïve"    = 8 bytes  (7 chars — 'ï' encodes as 2 bytes: 0xC3 0xAF), no trailing newline
    //   Total        = 27 bytes (23 chars)
    //
    // SplitIntoRangesFn runs with bundle size 7, forcing one shard per record. The shard byte
    // offsets flow directly into ReadFileShardFn without any manual override. If
    // SplitIntoRangesFn counted characters instead of bytes it would produce wrong offsets
    // (e.g. (0,16,2) instead of (0,8,1) and (8,19,1)), causing ReadFileShardFn to seek to
    // incorrect file positions and yield corrupted or missing records.
    Path inputFile = Files.createTempFile(testTableName, ".csv");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, StandardCharsets.UTF_8)) {
      writer.write("1,café\n2,résumé\n3,naïve");
    }

    PCollectionView<Map<String, String>> filesToTablesMapView =
        pipeline
            .apply("filesToTablesMapView", Create.of(KV.of(inputFile.toString(), testTableName)))
            .apply(View.asMap());

    PCollection<KV<String, CSVRecord>> records =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            .apply(FileIO.readMatches())
            .apply(
                "Split into ranges",
                ParDo.of(
                        new SplitIntoRangesFn(
                            7L,
                            filesToTablesMapView,
                            fieldQualifier,
                            columnDelimiter,
                            escapeChar,
                            handleNewLine))
                    .withSideInputs(filesToTablesMapView))
            .setCoder(FileShard.Coder.of())
            .apply(
                "Read lines",
                ParDo.of(
                    new ReadFileShardFn(
                        columnDelimiter,
                        fieldQualifier,
                        trailingDelimiter,
                        escapeChar,
                        nullString,
                        handleNewLine)));

    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));

    PAssert.that(csvValues)
        .containsInAnyOrder(
            KV.of(testTableName, "1,café"),
            KV.of(testTableName, "2,résumé"),
            KV.of(testTableName, "3,naïve"));

    pipeline.run();
  }

  @Test
  public void readFileWithNonAsciiChars() throws Exception {
    // Byte layout (UTF-8):
    //   "1,café\n"  = 8 bytes  (7 chars — 'é' encodes as 2 bytes: 0xC3 0xA9)
    //   "2,résumé"  = 10 bytes (8 chars — two 'é' characters), no trailing newline
    //   Total       = 18 bytes
    //
    // The shards are constructed with correct byte offsets. If the InputStreamReader in
    // ReadFileShardFn used a non-UTF-8 charset, the non-ASCII characters would be decoded
    // incorrectly and the assertions below would fail.
    Path inputFile = Files.createTempFile(testTableName, ".csv");
    try (BufferedWriter writer = Files.newBufferedWriter(inputFile, StandardCharsets.UTF_8)) {
      writer.write("1,café\n2,résumé");
    }

    PCollection<FileShard> fileShard =
        pipeline
            .apply("Create file name collection", Create.of(inputFile.toString()))
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            .apply(FileIO.readMatches())
            .apply(
                "Create file shard collection",
                ParDo.of(
                    new DoFn<FileIO.ReadableFile, FileShard>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(0L, 8L), 1L));
                        c.output(
                            FileShard.create(
                                testTableName, c.element(), new OffsetRange(8L, 18L), 1L));
                      }
                    }))
            .setCoder(FileShard.Coder.of());

    PCollection<KV<String, CSVRecord>> records =
        fileShard.apply(
            ParDo.of(
                new ReadFileShardFn(
                    columnDelimiter,
                    fieldQualifier,
                    trailingDelimiter,
                    escapeChar,
                    nullString,
                    handleNewLine)));

    PCollection<KV<String, String>> csvValues =
        records.apply(
            "get values",
            ParDo.of(
                new DoFn<KV<String, CSVRecord>, KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    CSVRecord csvRecord = c.element().getValue();
                    List<String> vals = new ArrayList<String>();
                    for (int i = 0; i < csvRecord.size(); i++) {
                      vals.add(csvRecord.get(i));
                    }
                    c.output(KV.of(c.element().getKey(), String.join(",", vals)));
                  }
                }));

    PAssert.that(csvValues)
        .containsInAnyOrder(
            KV.of(testTableName, "1,café"),
            KV.of(testTableName, "2,résumé"));

    pipeline.run();
  }

  public String csvRecordToValues(CSVRecord csvRecord) {
    List<String> vals = new ArrayList<String>();
    for (int i = 0; i < csvRecord.size(); i++) {
      vals.add(csvRecord.get(i));
    }
    return String.join(",", vals);
  }
}
