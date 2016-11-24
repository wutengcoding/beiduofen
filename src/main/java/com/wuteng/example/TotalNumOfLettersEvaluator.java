package com.wuteng.example;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * Created by wuteng1 on 2016/10/20.
 */
public class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator {

    PrimitiveObjectInspector inputOI;
    ObjectInspector outputOI;
    PrimitiveObjectInspector integerOI;

    int total = 0;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        assert (parameters.length == 1);
        super.init(m, parameters);

        // init input object inspectors
        if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
            inputOI = (PrimitiveObjectInspector) parameters[0];
        } else {
            integerOI = (PrimitiveObjectInspector) parameters[0];
        }

        // init output object inspectors
        outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        return outputOI;
    }


    static class LetterSumAgg implements AggregationBuffer{
        int sum = 0;
        void add(int num){
            sum += num;
        }
    }
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        LetterSumAgg result = new LetterSumAgg();
        return result;
    }

    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        LetterSumAgg myagg = new LetterSumAgg();
    }

    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
        assert (parameters.length == 1);
        if (parameters[0] != null) {
            LetterSumAgg myagg = (LetterSumAgg) agg;
            Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
            myagg.add(String.valueOf(p1).length());
        }
    }

    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
        LetterSumAgg myagg = (LetterSumAgg) agg;
        total += myagg.sum;
        return total;
    }

    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        if (partial != null) {

            LetterSumAgg myagg1 = (LetterSumAgg) agg;

            Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(partial);

            LetterSumAgg myagg2 = new LetterSumAgg();

            myagg2.add(partialSum);
            myagg1.add(myagg2.sum);
        }
    }

    public Object terminate(AggregationBuffer agg) throws HiveException {
        LetterSumAgg myagg = (LetterSumAgg) agg;
        total = myagg.sum;
        return myagg.sum;
    }
}
