package timelines;

import argo.profiles.OperationsParser;
import java.util.ArrayList;


public class TimelineMerger {

     private Timeline output=new Timeline();
     private ArrayList<Timeline> inputs=new ArrayList<>();

    public TimelineMerger() {
    }

   

     public void clear() {
        output.clear();
        inputs.clear();
    }

     public void aggregate(int[][][] truthTable, int op) {
        output.clear();
        inputs.clear();

        //Iterate through all available input timelines and aggregate
        for (Timeline item : inputs) {
            output.aggregate(item, truthTable, op);
        }
    }

     public void aggregate(ArrayList<Timeline> inputsT, int[][][] truthTable, int op) {

         if(output!=null )output.clear();
            if(inputs!=null) inputs.clear();

        inputs = inputsT;
        //Iterate through all available input timelines and aggregate
        for (Timeline item : inputs) {
            output.aggregate(item, truthTable,op);
        }
    }

     public Timeline getOutput() {
        return output;
    }

     public void setOutput(Timeline outputT) {
        output = outputT;
    }

     public ArrayList<Timeline> getInputs() {
        return inputs;
    }

     public void setInputs(ArrayList<Timeline> inputsT) {
        inputs = inputsT;
    }    

}
