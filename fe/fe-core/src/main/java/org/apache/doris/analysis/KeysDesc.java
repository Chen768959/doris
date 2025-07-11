// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

import com.google.common.collect.Lists;

import java.util.List;

public class KeysDesc {
    private KeysType type;
    private List<String> keysColumnNames;
    private List<String> clusterKeysColumnNames;

    public KeysDesc() {
        this.type = KeysType.AGG_KEYS;
        this.keysColumnNames = Lists.newArrayList();
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames) {
        this.type = type;
        this.keysColumnNames = keysColumnNames;
    }

    public KeysDesc(KeysType type, List<String> keysColumnNames, List<String> clusterKeyColumnNames) {
        this(type, keysColumnNames);
        this.clusterKeysColumnNames = clusterKeyColumnNames;
    }

    public KeysType getKeysType() {
        return type;
    }

    public int keysColumnSize() {
        return keysColumnNames.size();
    }

    public List<String> getClusterKeysColumnNames() {
        return clusterKeysColumnNames;
    }

    public boolean containsCol(String colName) {
        return keysColumnNames.contains(colName);
    }

    public void analyze(List<ColumnDef> cols) throws AnalysisException {
        if (type == null) {
            throw new AnalysisException("Keys type is null.");
        }

        if ((keysColumnNames == null || keysColumnNames.size() == 0) && type != KeysType.DUP_KEYS) {
            throw new AnalysisException("The number of key columns is 0.");
        }

        if (keysColumnNames.size() > cols.size()) {
            throw new AnalysisException("The number of key columns should be less than the number of columns.");
        }

        for (int i = 0; i < keysColumnNames.size(); ++i) {
            String name = cols.get(i).getName();
            if (!keysColumnNames.get(i).equalsIgnoreCase(name)) {
                String keyName = keysColumnNames.get(i);
                if (cols.stream().noneMatch(col -> col.getName().equalsIgnoreCase(keyName))) {
                    throw new AnalysisException("Key column[" + keyName + "] doesn't exist.");
                }
                throw new AnalysisException("Key columns should be a ordered prefix of the schema."
                        + " KeyColumns[" + i + "] (starts from zero) is " + keyName + ", "
                        + "but corresponding column is " + name  + " in the previous "
                        + "columns declaration.");
            }

            if (cols.get(i).getAggregateType() != null) {
                throw new AnalysisException("Key column[" + name + "] should not specify aggregate type.");
            }
        }

        // for olap table
        for (int i = keysColumnNames.size(); i < cols.size(); ++i) {
            if (type == KeysType.AGG_KEYS) {
                if (cols.get(i).getAggregateType() == null) {
                    throw new AnalysisException(type.name() + " table should specify aggregate type for "
                            + "non-key column[" + cols.get(i).getName() + "]");
                }
            } else {
                if (cols.get(i).getAggregateType() != null) {
                    throw new AnalysisException(type.name() + " table should not specify aggregate type for "
                            + "non-key column[" + cols.get(i).getName() + "]");
                }
            }
        }

        if (clusterKeysColumnNames != null) {
            analyzeClusterKeys(cols);
        }
    }

    private void analyzeClusterKeys(List<ColumnDef> cols) throws AnalysisException {
        if (type != KeysType.UNIQUE_KEYS) {
            throw new AnalysisException("Cluster keys only support unique keys table");
        }
        // check that cluster keys is not duplicated
        for (int i = 0; i < clusterKeysColumnNames.size(); i++) {
            String name = clusterKeysColumnNames.get(i);
            for (int j = 0; j < i; j++) {
                if (clusterKeysColumnNames.get(j).equalsIgnoreCase(name)) {
                    throw new AnalysisException("Duplicate cluster key column[" + name + "].");
                }
            }
        }
        // check that cluster keys is not equal to primary keys
        int minKeySize = Math.min(keysColumnNames.size(), clusterKeysColumnNames.size());
        boolean sameKey = true;
        for (int i = 0; i < minKeySize; i++) {
            if (!keysColumnNames.get(i).equalsIgnoreCase(clusterKeysColumnNames.get(i))) {
                sameKey = false;
                break;
            }
        }
        if (sameKey && !Config.random_add_cluster_keys_for_mow) {
            throw new AnalysisException("Unique keys and cluster keys should be different.");
        }
        // check that cluster key column exists
        for (int i = 0; i < clusterKeysColumnNames.size(); i++) {
            String name = clusterKeysColumnNames.get(i);
            for (int j = 0; j < cols.size(); j++) {
                if (cols.get(j).getName().equalsIgnoreCase(name)) {
                    cols.get(j).setClusterKeyId(i);
                    break;
                }
                if (j == cols.size() - 1) {
                    throw new AnalysisException("Cluster key column[" + name + "] doesn't exist.");
                }
            }
        }
    }

    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(type.toSql()).append("(");
        int i = 0;
        for (String columnName : keysColumnNames) {
            if (i != 0) {
                stringBuilder.append(", ");
            }
            stringBuilder.append("`").append(columnName).append("`");
            i++;
        }
        stringBuilder.append(")");
        if (clusterKeysColumnNames != null) {
            stringBuilder.append("\nCLUSTER BY (");
            i = 0;
            for (String columnName : clusterKeysColumnNames) {
                if (i != 0) {
                    stringBuilder.append(", ");
                }
                stringBuilder.append("`").append(columnName).append("`");
                i++;
            }
            stringBuilder.append(")");
        }
        return stringBuilder.toString();
    }
}
