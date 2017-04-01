package com.pilosa.client;

public enum TimeQuantum {
    NONE(0),
    YEAR(TimeQuantum.Y),
    MONTH(TimeQuantum.M),
    DAY(TimeQuantum.D),
    HOUR(TimeQuantum.H),
    YEAR_MONTH(TimeQuantum.Y | TimeQuantum.M),
    MONTH_DAY(TimeQuantum.M | TimeQuantum.D),
    DAY_HOUR(TimeQuantum.D | TimeQuantum.H),
    YEAR_MONTH_DAY(TimeQuantum.Y | TimeQuantum.M | TimeQuantum.D),
    MONTH_DAY_HOUR(TimeQuantum.M | TimeQuantum.D | TimeQuantum.H),
    YEAR_MONTH_DAY_HOUR(TimeQuantum.Y | TimeQuantum.M | TimeQuantum.D | TimeQuantum.H);

    public String getStringValue() {
        StringBuilder sb = new StringBuilder(4);
        if ((this.value & Y) == Y) sb.append('Y');
        if ((this.value & M) == M) sb.append('M');
        if ((this.value & D) == D) sb.append('D');
        if ((this.value & H) == H) sb.append('H');
        return sb.toString();
    }

    TimeQuantum(int value) {
        this.value = value;
    }

    private final int value;
    private static final byte Y = 0b00000001;
    private static final byte M = 0b00000010;
    private static final byte D = 0b00000100;
    private static final byte H = 0b00001000;
}
