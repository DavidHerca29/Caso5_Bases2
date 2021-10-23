package reducers;

public class Divisiones {
    private long monto;
    private String division;
    public Divisiones(String div,long mont) {
        this.division = div;
        this.monto = mont;
    }

    public long getMonto() {
        return monto;
    }

    public void setMonto(long monto) {
        this.monto = monto;
    }

    public String getDivision() {
        return division;
    }

    public void setDivision(String division) {
        this.division = division;
    }
}
