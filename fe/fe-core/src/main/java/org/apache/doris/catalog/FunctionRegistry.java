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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.annotation.Developing;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.BuiltinFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdafBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdtfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.UdfBuilder;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * New function registry for nereids.
 *
 * this class is developing for more functions.
 */
@Developing
@ThreadSafe
public class FunctionRegistry {

    // to record the global alias function and other udf.
    private static final String GLOBAL_FUNCTION = "__GLOBAL_FUNCTION__";

    private final Map<String, List<FunctionBuilder>> name2BuiltinBuilders;
    private final Map<String, Map<String, List<FunctionBuilder>>> name2UdfBuilders;

    public FunctionRegistry() {
        name2BuiltinBuilders = new ConcurrentHashMap<>();
        name2UdfBuilders = new ConcurrentHashMap<>();
        registerBuiltinFunctions(name2BuiltinBuilders);
        afterRegisterBuiltinFunctions(name2BuiltinBuilders);
    }

    public Map<String, List<FunctionBuilder>> getName2BuiltinBuilders() {
        return name2BuiltinBuilders;
    }

    public String getGlobalFunctionDbName() {
        return GLOBAL_FUNCTION;
    }

    public Map<String, Map<String, List<FunctionBuilder>>> getName2UdfBuilders() {
        return name2UdfBuilders;
    }

    // this function is used to test.
    // for example, you can create child class of FunctionRegistry and clear builtin functions or add more functions
    // in this method
    @VisibleForTesting
    protected void afterRegisterBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {}

    public FunctionBuilder findFunctionBuilder(String name, List<?> arguments) {
        return findFunctionBuilder(null, name, arguments);
    }

    public FunctionBuilder findFunctionBuilder(String name, Object argument) {
        return findFunctionBuilder(null, name, ImmutableList.of(argument));
    }

    public Optional<List<FunctionBuilder>> tryGetBuiltinBuilders(String name) {
        List<FunctionBuilder> builders = name2BuiltinBuilders.get(name);
        return name2BuiltinBuilders.get(name) == null
                ? Optional.empty()
                : Optional.of(ImmutableList.copyOf(builders));
    }

    public boolean isAggregateFunction(String dbName, String name) {
        name = name.toLowerCase();
        Class<?> aggClass = org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction.class;
        if (StringUtils.isEmpty(dbName)) {
            List<FunctionBuilder> functionBuilders = name2BuiltinBuilders.get(name);
            if (functionBuilders != null) {
                for (FunctionBuilder functionBuilder : functionBuilders) {
                    if (aggClass.isAssignableFrom(functionBuilder.functionClass())) {
                        return true;
                    }
                }
            }
        }

        List<FunctionBuilder> udfBuilders = findUdfBuilder(dbName, name);
        for (FunctionBuilder udfBuilder : udfBuilders) {
            if (aggClass.isAssignableFrom(udfBuilder.functionClass())) {
                return true;
            }
        }
        return false;
    }

    // currently we only find function by name and arity and args' types.
    public FunctionBuilder findFunctionBuilder(String dbName, String name, List<?> arguments) {
        List<FunctionBuilder> functionBuilders = null;
        int arity = arguments.size();
        String qualifiedName = StringUtils.isEmpty(dbName) ? name : dbName + "." + name;

        boolean preferUdfOverBuiltin = ConnectContext.get() == null ? false
                : ConnectContext.get().getSessionVariable().preferUdfOverBuiltin;

        if (preferUdfOverBuiltin) {
            // find udf first, then find builtin function
            functionBuilders = findUdfBuilder(dbName, name);
            if (CollectionUtils.isEmpty(functionBuilders) && StringUtils.isEmpty(dbName)) {
                // if dbName is not empty, we should search builtin functions first
                functionBuilders = findBuiltinFunctionBuilder(name, arguments);
            }
        } else {
            // find builtin function first, then find udf
            if (StringUtils.isEmpty(dbName)) {
                functionBuilders = findBuiltinFunctionBuilder(name, arguments);
            }
            if (CollectionUtils.isEmpty(functionBuilders)) {
                functionBuilders = findUdfBuilder(dbName, name);
            }
        }

        if (functionBuilders == null || functionBuilders.isEmpty()) {
            throw new AnalysisException("Can not found function '" + qualifiedName + "'");
        }

        // check the arity and type
        List<FunctionBuilder> candidateBuilders = Lists.newArrayListWithCapacity(arguments.size());
        for (FunctionBuilder functionBuilder : functionBuilders) {
            if (functionBuilder.canApply(arguments)) {
                candidateBuilders.add(functionBuilder);
            }
        }
        if (candidateBuilders.isEmpty()) {
            String candidateHints = getCandidateHint(name, functionBuilders);
            throw new AnalysisException("Can not found function '" + qualifiedName
                    + "' which has " + arity + " arity. Candidate functions are: " + candidateHints);
        }
        if (!Config.enable_java_udf) {
            candidateBuilders = candidateBuilders.stream()
                    .filter(fb -> !(fb instanceof JavaUdfBuilder || fb instanceof JavaUdafBuilder
                            || fb instanceof JavaUdtfBuilder))
                    .collect(Collectors.toList());
            if (candidateBuilders.isEmpty()) {
                throw new AnalysisException("java_udf has been disabled.");
            }
        }
        if (candidateBuilders.size() > 1) {
            boolean needChooseOne = true;
            List<FunctionSignature> signatures = Lists.newArrayListWithCapacity(candidateBuilders.size());
            for (FunctionBuilder functionBuilder : candidateBuilders) {
                if (functionBuilder instanceof UdfBuilder) {
                    signatures.addAll(((UdfBuilder) functionBuilder).getSignatures());
                } else {
                    needChooseOne = false;
                    break;
                }
            }
            for (Object argument : arguments) {
                if (!(argument instanceof Expression)) {
                    needChooseOne = false;
                    break;
                }
            }
            if (needChooseOne) {
                FunctionSignature signature = new UdfSignatureSearcher(signatures, (List) arguments).getSignature();
                for (int i = 0; i < signatures.size(); i++) {
                    if (signatures.get(i).equals(signature)) {
                        return candidateBuilders.get(i);
                    }
                }
            }
            String candidateHints = getCandidateHint(name, candidateBuilders);
            throw new AnalysisException("Function '" + qualifiedName + "' is ambiguous: " + candidateHints);
        }
        return candidateBuilders.get(0);
    }

    private List<FunctionBuilder> findBuiltinFunctionBuilder(String name, List<?> arguments) {
        List<FunctionBuilder> functionBuilders;
        // search internal function only if dbName is empty
        functionBuilders = name2BuiltinBuilders.get(name.toLowerCase());
        if (CollectionUtils.isEmpty(functionBuilders) && AggCombinerFunctionBuilder.isAggStateCombinator(name)) {
            String nestedName = AggCombinerFunctionBuilder.getNestedName(name);
            String combinatorSuffix = AggCombinerFunctionBuilder.getCombinatorSuffix(name);
            functionBuilders = name2BuiltinBuilders.get(nestedName.toLowerCase());
            if (functionBuilders != null) {
                List<FunctionBuilder> candidateBuilders = Lists.newArrayListWithCapacity(functionBuilders.size());
                for (FunctionBuilder functionBuilder : functionBuilders) {
                    AggCombinerFunctionBuilder combinerBuilder
                            = new AggCombinerFunctionBuilder(combinatorSuffix, functionBuilder);
                    if (combinerBuilder.canApply(arguments)) {
                        candidateBuilders.add(combinerBuilder);
                    }
                }
                functionBuilders = candidateBuilders;
            }
        }
        return functionBuilders;
    }

    /**
     * public for test.
     */
    public List<FunctionBuilder> findUdfBuilder(String dbName, String name) {
        List<String> scopes = ImmutableList.of(GLOBAL_FUNCTION);
        if (ConnectContext.get() != null) {
            dbName = dbName == null ? ConnectContext.get().getDatabase() : dbName;
            if (dbName == null || !Env.getCurrentEnv().getAccessManager()
                    .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                            PrivPredicate.SELECT)) {
                scopes = ImmutableList.of(GLOBAL_FUNCTION);
            } else {
                scopes = ImmutableList.of(dbName, GLOBAL_FUNCTION);
            }
        }

        synchronized (name2UdfBuilders) {
            for (String scope : scopes) {
                List<FunctionBuilder> candidate = name2UdfBuilders.getOrDefault(scope, ImmutableMap.of())
                        .get(name.toLowerCase());
                if (candidate != null && !candidate.isEmpty()) {
                    return candidate;
                }
            }
        }
        return ImmutableList.of();
    }

    private void registerBuiltinFunctions(Map<String, List<FunctionBuilder>> name2Builders) {
        FunctionHelper.addFunctions(name2Builders, BuiltinScalarFunctions.INSTANCE.scalarFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinAggregateFunctions.INSTANCE.aggregateFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinTableValuedFunctions.INSTANCE.tableValuedFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinTableGeneratingFunctions.INSTANCE.tableGeneratingFunctions);
        FunctionHelper.addFunctions(name2Builders, BuiltinWindowFunctions.INSTANCE.windowFunctions);
    }

    public String getCandidateHint(String name, List<FunctionBuilder> candidateBuilders) {
        return candidateBuilders.stream()
                .filter(builder -> {
                    if (builder instanceof BuiltinFunctionBuilder) {
                        Constructor<BoundFunction> builderMethod
                                = ((BuiltinFunctionBuilder) builder).getBuilderMethod();
                        if (Modifier.isAbstract(builderMethod.getModifiers())
                                || !Modifier.isPublic(builderMethod.getModifiers())) {
                            return false;
                        }
                        for (Class<?> parameterType : builderMethod.getParameterTypes()) {
                            if (!Expression.class.isAssignableFrom(parameterType)
                                    && !(parameterType.isArray()
                                        && Expression.class.isAssignableFrom(parameterType.getComponentType()))) {
                                return false;
                            }
                        }
                    }
                    return true;
                })
                .map(builder -> name + builder.parameterDisplayString())
                .collect(Collectors.joining(", ", "[", "]"));
    }

    public void addUdf(String dbName, String name, UdfBuilder builder) {
        if (dbName == null) {
            dbName = GLOBAL_FUNCTION;
        }
        synchronized (name2UdfBuilders) {
            Map<String, List<FunctionBuilder>> builders = name2UdfBuilders
                    .computeIfAbsent(dbName, k -> Maps.newHashMap());
            builders.computeIfAbsent(name, k -> Lists.newArrayList()).add(builder);
        }
    }

    public void dropUdf(String dbName, String name, List<DataType> argTypes) {
        if (dbName == null) {
            dbName = GLOBAL_FUNCTION;
        }
        synchronized (name2UdfBuilders) {
            Map<String, List<FunctionBuilder>> builders = name2UdfBuilders.getOrDefault(dbName, ImmutableMap.of());
            builders.getOrDefault(name, Lists.newArrayList())
                    .removeIf(builder -> ((UdfBuilder) builder).getArgTypes().equals(argTypes));

            // the name will be used when show functions, so remove the name when it's dropped
            if (builders.getOrDefault(name, Lists.newArrayList()).isEmpty()) {
                builders.remove(name);
            }
        }
    }

    /**
     * use for search appropriate signature for UDFs if candidate more than one.
     */
    static class UdfSignatureSearcher implements ExplicitlyCastableSignature {

        private final List<FunctionSignature> signatures;
        private final List<Expression> arguments;

        public UdfSignatureSearcher(List<FunctionSignature> signatures, List<Expression> arguments) {
            this.signatures = signatures;
            this.arguments = arguments;
        }

        @Override
        public List<FunctionSignature> getSignatures() {
            return signatures;
        }

        @Override
        public FunctionSignature getSignature() {
            return searchSignature(signatures);
        }

        @Override
        public boolean nullable() {
            throw new AnalysisException("could not call nullable on UdfSignatureSearcher");
        }

        @Override
        public List<Expression> children() {
            return arguments;
        }

        @Override
        public Expression child(int index) {
            return arguments.get(index);
        }

        @Override
        public int arity() {
            return arguments.size();
        }

        @Override
        public <T> Optional<T> getMutableState(String key) {
            return Optional.empty();
        }

        @Override
        public void setMutableState(String key, Object value) {
        }

        @Override
        public Expression withChildren(List<Expression> children) {
            throw new AnalysisException("could not call withChildren on UdfSignatureSearcher");
        }
    }
}
