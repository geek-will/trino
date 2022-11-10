/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiFile;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.HudiTableType;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiUtil.getFileStatus;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
    }

    public Stream<HudiSplit> createSplits(
            String timeLine,
            FileSlice fileSlice,
            HudiPartitionInfo partition,
            List<HivePartitionKey> partitionKeys)
    {
        Option<HoodieBaseFile> baseFile = fileSlice.getBaseFile();
        if (!baseFile.isPresent() && hudiTableHandle.getTableType() == HudiTableType.COW) {
            return Stream.empty();
        }

        List<HudiFile> logFiles = fileSlice.getLogFiles()
                .map(logFileStatus -> HudiFile.fromFileStatus(logFileStatus.getFileStatus()))
                .collect(toImmutableList());
        if (!baseFile.isPresent() && logFiles.size() == 0) {
            return Stream.empty();
        }

        FileStatus fileStatus = getFileStatus(baseFile.get());
        List<FileSplit> splits;
        try {
            splits = createSplits(fileStatus);
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, e);
        }

        // Distribute log files equally to baseFileSplits
        List<HudiSplit> hudiSplits = new ArrayList<>();
        int step = splits.isEmpty() ? logFiles.size() : (int) Math.ceil((double) logFiles.size() / (double) splits.size());
        for (int i = 0; i < splits.size(); i++) {
            int start = i * step;
            int end = (i + 1) * step > logFiles.size() ? logFiles.size() : (i + 1) * step;
            FileSplit baseFileSplit = splits.get(i);
            List<HudiFile> targetLogFiles = logFiles.subList(start, end);
            final long sizeInBytes;
            final Optional<HudiFile> optBaseHudiFile;
            if (baseFileSplit.getPath() == null || baseFileSplit.getStart() < 0 || baseFileSplit.getLength() <= 0) {
                optBaseHudiFile = Optional.empty();
                sizeInBytes = targetLogFiles.size() > 0 ? targetLogFiles.stream().map(HudiFile::getLength).reduce(0L, Long::sum) : 0L;
            }
            else {
                optBaseHudiFile = Optional.of(HudiFile.fromFileSplit(baseFileSplit));
                sizeInBytes = baseFileSplit.getLength();
            }

            hudiSplits.add(new HudiSplit(
                    partition.getTable(),
                    fileStatus.getModificationTime(),
                    optBaseHudiFile,
                    targetLogFiles,
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(sizeInBytes),
                    timeLine));
        }
        return hudiSplits.stream();
    }

    private List<FileSplit> createSplits(FileStatus fileStatus)
            throws IOException
    {
        if (fileStatus.isDirectory()) {
            throw new IOException("Not a file: " + fileStatus.getPath());
        }

        Path path = fileStatus.getPath();
        long length = fileStatus.getLen();

        if (length == 0) {
            return ImmutableList.of(new FileSplit(path, 0, 0, new String[0]));
        }

        if (!isSplitable(path)) {
            return ImmutableList.of(new FileSplit(path, 0, length, (String[]) null));
        }

        ImmutableList.Builder<FileSplit> splits = ImmutableList.builder();
        long splitSize = fileStatus.getBlockSize();

        long bytesRemaining = length;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            splits.add(new FileSplit(path, length - bytesRemaining, splitSize, (String[]) null));
            bytesRemaining -= splitSize;
        }
        if (bytesRemaining != 0) {
            splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, (String[]) null));
        }
        return splits.build();
    }

    private static boolean isSplitable(Path filename)
    {
        return !(filename instanceof PathWithBootstrapFileStatus);
    }
}
