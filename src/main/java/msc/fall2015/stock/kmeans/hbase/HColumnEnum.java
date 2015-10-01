package msc.fall2015.stock.kmeans.hbase;

public enum HColumnEnum {
    SRV_COL_employeeid ("employeeid".getBytes()),
    SRV_COL_eventdesc ("eventdesc".getBytes()),
    SRV_COL_eventdate ("eventdate".getBytes()),
    SRV_COL_objectname ("objectname".getBytes()),
    SRV_COL_objectfolder ("objectfolder".getBytes()),
    SRV_COL_ipaddress ("ipaddress".getBytes());

    private final byte[] columnName;

    HColumnEnum (byte[] column) {
        this.columnName = column;
    }

    public byte[] getColumnName() {
        return this.columnName;
    }
}
