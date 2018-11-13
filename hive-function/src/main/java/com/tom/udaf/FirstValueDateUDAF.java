package com.tom.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FirstValueDateUDAF extends AbstractGenericUDAFResolver {

    static final Logger logger = LoggerFactory.getLogger(FirstValueDateUDAF.class);

    /**
     * 做类型和参数长度验证
     *
     * @param parameters
     * @return
     * @throws SemanticException
     */
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters == null) {
            new FirstValueByDateEvaluator();
        }

        ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);

        if (oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be PRIMITIVE, but "
                            + oi.getCategory().name()
                            + " was passed.");
        }

        PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;

        if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0,
                    "Argument must be String, but "
                            + inputOI.getPrimitiveCategory().name()
                            + " was passed.");
        }
        logger.info("create udaf");
        return new FirstValueByDateEvaluator();
    }

    public static class FirstValueByDateEvaluator extends GenericUDAFEvaluator {

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            ObjectInspector outputOI;
            // 指定各个阶段输出数据格式都为String类型
            outputOI = ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                    ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return outputOI;
        }

        /**
         * 存储根据时间得到的类
         */
        static class InfoAgg implements AggregationBuffer {
            String info;
            boolean ascFlag = true;
            public void reset() {
                logger.info("wow invoke the reset");
                ascFlag = true;
                info = "";
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() {
            return new InfoAgg();
        }

        @Override
        public void reset(AggregationBuffer agg) {
            InfoAgg infoAgg = (InfoAgg) agg;
            infoAgg.reset();
        }

        /**
         * 遍历参数
         *
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) {
            if (parameters != null) {
                InfoAgg infoAgg = (InfoAgg) agg;
                if (parameters.length == 2) {
                    infoAgg.ascFlag = "asc".equalsIgnoreCase(parameters[1].toString().trim());
                }
                if (parameters[0] != null) {
                    if (infoAgg.info != null && !infoAgg.info.isEmpty()) {
                        String dataInfo = parameters[0].toString();
                        rankVal(infoAgg, dataInfo);
                    } else {
                        infoAgg.info = parameters[0].toString();
                    }
                }
            }
        }

        private void rankVal(InfoAgg infoAgg, String dataInfo) {
            //            拿出 yyyy-MM-dd 这种类型的日期
                int dat = Integer.parseInt(dataInfo.split("\\,")[0].replace("-", ""));
                if (infoAgg.info == null || infoAgg.info.isEmpty()) {
                    infoAgg.info = dataInfo;
                } else {
                    int aggDat = Integer.parseInt(infoAgg.info.split("\\,")[0].replace("-", ""));
                    if ((aggDat > dat && infoAgg.ascFlag) || (aggDat < dat && !infoAgg.ascFlag)) {
                        infoAgg.info = dataInfo;
                    }
                }
        }

        /**
         * 部分map的返回结果
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) {
            InfoAgg infoAgg = (InfoAgg) agg;
            return infoAgg.info;
        }

        /**
         * 合并
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) {
            InfoAgg infoAgg = (InfoAgg) agg;
            if (partial != null) {
                String dataInfo = partial.toString();
//            拿出 yyyy-MM-dd 这种类型的日期
                rankVal(infoAgg, dataInfo);
            }
        }

        /**
         * 最终汇总
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) {
            InfoAgg infoAgg = (InfoAgg) agg;
            return terminatePartial(agg);
        }
        // UDAF logic goes here!
    }
}
