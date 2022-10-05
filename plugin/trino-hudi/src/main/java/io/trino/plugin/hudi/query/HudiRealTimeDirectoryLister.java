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

package io.trino.plugin.hudi.query;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class HudiRealTimeDirectoryLister
        implements HudiDirectoryLister
{
    private final Table hiveTable;
    private final SchemaTableName tableName;
    private final HudiTableHandle tableHandle;
    private final HiveMetastore hiveMetastore;
    private final List<HiveColumnHandle> partitionColumnHandles;
    private final HoodieTableMetaClient tableMetaClient;
    private final HoodieTableFileSystemView fileSystemView;
    private final TupleDomain<String> partitionKeysFilter;
    private List<String> hivePartitionNames;
    private final List<Column> partitionColumns;

    public HudiRealTimeDirectoryLister(
            Table hiveTable,
            HudiTableHandle tableHandle,
            HiveMetastore hiveMetastore,
            HoodieEngineContext engineContext,
            HoodieMetadataConfig metadataConfig,
            HoodieTableMetaClient tableMetaClient,
            List<HiveColumnHandle> partitionColumnHandles)
    {
        this.hiveTable = hiveTable;
        this.tableHandle = tableHandle;
        this.hiveMetastore = hiveMetastore;
        this.tableName = tableHandle.getSchemaTableName();
        this.partitionColumns = hiveTable.getPartitionColumns();
        this.partitionColumnHandles = partitionColumnHandles;
        this.tableMetaClient = tableMetaClient;
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, tableMetaClient, metadataConfig);
        this.partitionKeysFilter = MetastoreUtil.computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates());
    }

    @Override
    public List<HudiPartitionInfo> getPartitionsToScan()
    {
        if (hivePartitionNames == null) {
            hivePartitionNames = partitionColumns.isEmpty()
                    ? Collections.singletonList("")
                    : getPartitionNamesFromHiveMetastore(partitionKeysFilter);
        }

        List<HudiPartitionInfo> allPartitionInfoList = hivePartitionNames.stream()
                .map(hivePartitionName -> new HiveHudiPartitionInfo(
                        hivePartitionName,
                        partitionColumns,
                        partitionColumnHandles,
                        tableHandle.getPartitionPredicates(),
                        hiveTable,
                        hiveMetastore))
                .collect(Collectors.toList());

        return allPartitionInfoList.stream()
                .filter(partitionInfo -> partitionInfo.getHivePartitionKeys().isEmpty() || partitionInfo.doesMatchPredicates())
                .collect(Collectors.toList());
    }

    @Override
    public List<FileSlice> listFileSlice(HudiPartitionInfo partitionInfo)
    {
        HoodieTimeline timeline = tableMetaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
        String timestamp = timeline.lastInstant().map(HoodieInstant::getTimestamp).orElse(null);
        if (timestamp == null) {
            // no completed instant for current table
            return ImmutableList.of();
        }
        return fileSystemView.getLatestFileSlicesBeforeOrOn(partitionInfo.getRelativePartitionPath(), timestamp, false)
                .collect(Collectors.toList());
    }

    @Override
    public Map<String, Optional<Partition>> getPartitions(List<String> partitionNames)
    {
        return hiveMetastore.getPartitionsByNames(hiveTable, partitionNames);
    }

    @Override
    public void close()
    {
        if (fileSystemView != null && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }

    private List<String> getPartitionNamesFromHiveMetastore(TupleDomain<String> partitionKeysFilter)
    {
        return hiveMetastore.getPartitionNamesByFilter(
                tableName.getSchemaName(),
                tableName.getTableName(),
                partitionColumns.stream().map(Column::getName).collect(Collectors.toList()),
                partitionKeysFilter).orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
    }
}
