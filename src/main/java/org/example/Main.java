package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kudu.connector.converter.RowResultRowConverter;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.KuduSource;
import org.apache.flink.connector.kudu.source.KuduSourceBuilder;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class Main {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.enableCheckpointing(5000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            env.setStateBackend(new FsStateBackend("file:///tmp/flink-checkpoints"));
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
            );

            KuduSource<Row> kuduSource =
                    new KuduSourceBuilder<Row>()
                            .setReaderConfig(getReaderConfig())
                            .setTableInfo(getTableInfo())
                            .setRowResultConverter(new RowResultRowConverter())
                            .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                            .setDiscoveryPeriod(Duration.ofSeconds(1))
                            .build();

            // Read from Kudu and print
            env.fromSource(kuduSource, WatermarkStrategy.noWatermarks(), "KuduSource")
                    .returns(TypeInformation.of(Row.class))
                    .print();

            env.execute("Kudu Flink Job");
        }

        private static KuduReaderConfig getReaderConfig() {
            return KuduReaderConfig.Builder
                    .setMasters("localhost:8764")
                    .build();
        }

        private static org.apache.flink.connector.kudu.connector.KuduTableInfo getTableInfo() {
            return org.apache.flink.connector.kudu.connector.KuduTableInfo
                    .forTable("test_table");
        }
}