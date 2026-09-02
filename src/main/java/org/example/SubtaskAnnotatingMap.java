package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;

public class SubtaskAnnotatingMap extends RichMapFunction<String, String> {
    @Override
    public String map(String message) throws Exception {
//        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int numberOfSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        return String.format("Subtask %d of %d got %s", subtaskIndex+1, numberOfSubtasks, message);
    }
}
