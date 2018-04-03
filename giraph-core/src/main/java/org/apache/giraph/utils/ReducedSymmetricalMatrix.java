package org.apache.giraph.utils;

public class ReducedSymmetricalMatrix {

    private int numZeroEntries;

    private int matrixDim;

    private int numEntries;

    private long[] matrix;

    public ReducedSymmetricalMatrix(int matrixDim) {
        this.matrixDim = matrixDim;
        this.numEntries = ((matrixDim * matrixDim) - matrixDim)/2;
        this.numZeroEntries = numEntries;
        this.matrix = new long[this.numEntries];
    }

    public void addValue(int cl, int l, long value){

        int idx = convertIndex(cl, l);

        long previousValue = this.matrix[idx];

        this.matrix[idx] = previousValue + value;

        if(previousValue == 0 && value > 0){
            this.numZeroEntries--;
        }
    }

    public long readValue(int cl, int l){
        return this.matrix[convertIndex(cl,l)];
    }

    public int numNonZeroEntries(){
        return this.numEntries - this.numZeroEntries;
    }

    private int convertIndex(int cl, int l) {
        int x,y;

        if(cl < l){
            x = cl;
            y = l;
        }
        else {
            x = l;
            y = cl;
        }

        return x * matrixDim - ((((x+1) * (x+1)) - (x+1))/2) + (y - (x+1));
    }
}
