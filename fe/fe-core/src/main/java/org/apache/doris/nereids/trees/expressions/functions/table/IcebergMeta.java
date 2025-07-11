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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.tablefunction.IcebergTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/** iceberg_meta */
public class IcebergMeta extends TableValuedFunction {
    public IcebergMeta(Properties properties) {
        super("iceberg_meta", properties);
    }

    public static IcebergMeta createIcebergMeta(List<String> nameParts, String queryType) {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(IcebergTableValuedFunction.TABLE, Joiner.on(".").join(nameParts));
        prop.put(IcebergTableValuedFunction.QUERY_TYPE, queryType);
        return new IcebergMeta(new Properties(prop));
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(AnyDataType.INSTANCE_WITHOUT_INDEX, getArgumentsTypes());
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            Map<String, String> arguments = getTVFProperties().getMap();
            return IcebergTableValuedFunction.create(arguments);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build IcebergTableValuedFunction by "
                    + this + ": " + t.getMessage(), t);
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitIcebergMeta(this, context);
    }
}
