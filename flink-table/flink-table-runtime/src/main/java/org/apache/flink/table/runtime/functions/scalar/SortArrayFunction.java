package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Comparator;

import static org.apache.flink.table.api.Expressions.$;

public class SortArrayFunction extends BuiltInScalarFunction {
    private final ArrayData.ElementGetter elementGetter;
    private final SpecializedFunction.ExpressionEvaluator equalityEvaluator;
    private final SpecializedFunction.ExpressionEvaluator compareEvaluator;
    private MethodHandle equalityHandle;
    private MethodHandle compareHandle;

    public SortArrayFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.SORT_ARRAY, context);
        final DataType dataType =
                ((CollectionDataType) context.getCallContext().getArgumentDataTypes().get(0))
                        .getElementDataType();
        elementGetter = ArrayData.createElementGetter(dataType.getLogicalType());

        equalityEvaluator =
                context.createEvaluator(
                        $("element1").isEqual($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", dataType.notNull().toInternal()));

        compareEvaluator =
                context.createEvaluator(
                        $("element1").isGreater($("element2")),
                        DataTypes.BOOLEAN().notNull(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", dataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        equalityHandle = equalityEvaluator.open(context);
        compareHandle = compareEvaluator.open(context);
    }

    public @Nullable ArrayData eval(ArrayData haystack) {
        if (haystack == null) {
            return null;
        }
        if (haystack.size() <= 1) {
            return haystack;
        }
        Object[] array = new Object[haystack.size()];
        for (int i = 0; i < haystack.size(); i++) {
            array[i] = elementGetter.getElementOrNull(haystack, i);
        }

        // TODO: How to sort null elements?
        Comparator comparator = new Comparator() {
            @Override
            public int compare(Object element1, Object element2) {

                try {
                    if ((boolean) equalityHandle.invoke(element1, element2)) {
                        return 0;
                    }
                    else if ((boolean) compareHandle.invoke(element1, element2)) {
                        return 1;
                    }
                    else {
                        return -1;
                    }
                } catch (Throwable t) {
                    throw new FlinkRuntimeException(t);
                }
            }
        };

        // TODO: Pass in ascending/descending value
        Arrays.sort(array, comparator);

        return new GenericArrayData(array);
    }

    @Override
    public void close() throws Exception {
        equalityEvaluator.close();
        compareEvaluator.close();
    }
}
