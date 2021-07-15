package timelines;

import java.util.ArrayList;
import ops.OpsManager;

public class TimelineMerger {

    static private Timeline output=new Timeline();
    static private ArrayList<Timeline> inputs=new ArrayList<>();

    static public void clear() {
        output.clear();
        inputs.clear();
    }

    static public void aggregate(int[][][] truthTable, int op) {
        output.clear();
        inputs.clear();

        //Iterate through all available input timelines and aggregate
        for (Timeline item : inputs) {
            output.aggregate(item, truthTable, op);
        }
    }

    static public void aggregate(ArrayList<Timeline> inputsT, int[][][] truthTable, int op) {
        output.clear();
        inputs.clear();

        inputs = inputsT;
        //Iterate through all available input timelines and aggregate
        for (Timeline item : inputs) {
            output.aggregate(item, truthTable, op);
        }
    }

    static public Timeline getOutput() {
        return output;
    }

    static public void setOutput(Timeline outputT) {
        output = outputT;
    }

    static public ArrayList<Timeline> getInputs() {
        return inputs;
    }

    static public void setInputs(ArrayList<Timeline> inputsT) {
        inputs = inputsT;
    }

}
