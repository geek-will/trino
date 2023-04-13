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
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HudiSplit
        implements ConnectorSplit
{
    private final Table table;
    private final long fileModifiedTime;
    private final Optional<HudiFile> baseFile;
    private final List<HudiFile> logFiles;
    private final String instantTime;
    private final List<HostAddress> addresses;
    private final TupleDomain<HiveColumnHandle> predicate;
    private final List<HivePartitionKey> partitionKeys;
    private final SplitWeight splitWeight;

    @JsonCreator
    public HudiSplit(
            @JsonProperty("table") Table table,
            @JsonProperty("fileModifiedTime") long fileModifiedTime,
            @JsonProperty("baseFile") Optional<HudiFile> baseFile,
            @JsonProperty("logFiles") List<HudiFile> logFiles,
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("predicate") TupleDomain<HiveColumnHandle> predicate,
            @JsonProperty("partitionKeys") List<HivePartitionKey> partitionKeys,
            @JsonProperty("splitWeight") SplitWeight splitWeight,
            @JsonProperty("instantTime") String instantTime)
    {
        this.table = requireNonNull(table, "table is null");
        this.fileModifiedTime = fileModifiedTime;
        this.baseFile = requireNonNull(baseFile, "baseFile is null");
        this.logFiles = requireNonNull(logFiles, "logFiles is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.partitionKeys = ImmutableList.copyOf(requireNonNull(partitionKeys, "partitionKeys is null"));
        this.splitWeight = requireNonNull(splitWeight, "splitWeight is null");
        this.instantTime = requireNonNull(instantTime, "instantTime is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("fileModifiedTime", fileModifiedTime)
                .put("baseFile", baseFile)
                .put("logFiles", logFiles)
                .buildOrThrow();
    }

    @JsonProperty
    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @JsonProperty
    public Table getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<HudiFile> getBaseFile()
    {
        return baseFile;
    }

    @JsonProperty
    public List<HudiFile> getLogFiles()
    {
        return logFiles;
    }

    @JsonProperty
    public long getFileModifiedTime()
    {
        return fileModifiedTime;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public List<HivePartitionKey> getPartitionKeys()
    {
        return partitionKeys;
    }

    @JsonProperty
    public String getInstantTime()
    {
        return instantTime;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(fileModifiedTime)
                .add("baseFile", baseFile)
                .add("logFiles", logFiles)
                .toString();
    }
}
