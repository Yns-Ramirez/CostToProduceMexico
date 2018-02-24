// ORM class for table 'null'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Tue Jul 25 13:05:05 CDT 2017
// For connector: org.apache.sqoop.manager.GenericJdbcManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class QueryResult extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("TipoNomina_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TipoNomina_ID = (String)value;
      }
    });
    setters.put("Empleado_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Empleado_ID = (String)value;
      }
    });
    setters.put("FechaPago", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FechaPago = (java.sql.Date)value;
      }
    });
    setters.put("CuentaNatural_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        CuentaNatural_ID = (String)value;
      }
    });
    setters.put("AnalisisLocal_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        AnalisisLocal_ID = (String)value;
      }
    });
    setters.put("Concepto_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Concepto_ID = (Integer)value;
      }
    });
    setters.put("Region_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Region_ID = (String)value;
      }
    });
    setters.put("MontoPago", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        MontoPago = (Double)value;
      }
    });
    setters.put("TipoMoneda_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TipoMoneda_ID = (Integer)value;
      }
    });
    setters.put("SistemaFuente", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        SistemaFuente = (String)value;
      }
    });
    setters.put("UsuarioETL", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        UsuarioETL = (String)value;
      }
    });
    setters.put("FechaCarga", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FechaCarga = (java.sql.Timestamp)value;
      }
    });
    setters.put("FechaCambio", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        FechaCambio = (java.sql.Timestamp)value;
      }
    });
  }
  public QueryResult() {
    init0();
  }
  private String TipoNomina_ID;
  public String get_TipoNomina_ID() {
    return TipoNomina_ID;
  }
  public void set_TipoNomina_ID(String TipoNomina_ID) {
    this.TipoNomina_ID = TipoNomina_ID;
  }
  public QueryResult with_TipoNomina_ID(String TipoNomina_ID) {
    this.TipoNomina_ID = TipoNomina_ID;
    return this;
  }
  private String Empleado_ID;
  public String get_Empleado_ID() {
    return Empleado_ID;
  }
  public void set_Empleado_ID(String Empleado_ID) {
    this.Empleado_ID = Empleado_ID;
  }
  public QueryResult with_Empleado_ID(String Empleado_ID) {
    this.Empleado_ID = Empleado_ID;
    return this;
  }
  private java.sql.Date FechaPago;
  public java.sql.Date get_FechaPago() {
    return FechaPago;
  }
  public void set_FechaPago(java.sql.Date FechaPago) {
    this.FechaPago = FechaPago;
  }
  public QueryResult with_FechaPago(java.sql.Date FechaPago) {
    this.FechaPago = FechaPago;
    return this;
  }
  private String CuentaNatural_ID;
  public String get_CuentaNatural_ID() {
    return CuentaNatural_ID;
  }
  public void set_CuentaNatural_ID(String CuentaNatural_ID) {
    this.CuentaNatural_ID = CuentaNatural_ID;
  }
  public QueryResult with_CuentaNatural_ID(String CuentaNatural_ID) {
    this.CuentaNatural_ID = CuentaNatural_ID;
    return this;
  }
  private String AnalisisLocal_ID;
  public String get_AnalisisLocal_ID() {
    return AnalisisLocal_ID;
  }
  public void set_AnalisisLocal_ID(String AnalisisLocal_ID) {
    this.AnalisisLocal_ID = AnalisisLocal_ID;
  }
  public QueryResult with_AnalisisLocal_ID(String AnalisisLocal_ID) {
    this.AnalisisLocal_ID = AnalisisLocal_ID;
    return this;
  }
  private Integer Concepto_ID;
  public Integer get_Concepto_ID() {
    return Concepto_ID;
  }
  public void set_Concepto_ID(Integer Concepto_ID) {
    this.Concepto_ID = Concepto_ID;
  }
  public QueryResult with_Concepto_ID(Integer Concepto_ID) {
    this.Concepto_ID = Concepto_ID;
    return this;
  }
  private String Region_ID;
  public String get_Region_ID() {
    return Region_ID;
  }
  public void set_Region_ID(String Region_ID) {
    this.Region_ID = Region_ID;
  }
  public QueryResult with_Region_ID(String Region_ID) {
    this.Region_ID = Region_ID;
    return this;
  }
  private Double MontoPago;
  public Double get_MontoPago() {
    return MontoPago;
  }
  public void set_MontoPago(Double MontoPago) {
    this.MontoPago = MontoPago;
  }
  public QueryResult with_MontoPago(Double MontoPago) {
    this.MontoPago = MontoPago;
    return this;
  }
  private Integer TipoMoneda_ID;
  public Integer get_TipoMoneda_ID() {
    return TipoMoneda_ID;
  }
  public void set_TipoMoneda_ID(Integer TipoMoneda_ID) {
    this.TipoMoneda_ID = TipoMoneda_ID;
  }
  public QueryResult with_TipoMoneda_ID(Integer TipoMoneda_ID) {
    this.TipoMoneda_ID = TipoMoneda_ID;
    return this;
  }
  private String SistemaFuente;
  public String get_SistemaFuente() {
    return SistemaFuente;
  }
  public void set_SistemaFuente(String SistemaFuente) {
    this.SistemaFuente = SistemaFuente;
  }
  public QueryResult with_SistemaFuente(String SistemaFuente) {
    this.SistemaFuente = SistemaFuente;
    return this;
  }
  private String UsuarioETL;
  public String get_UsuarioETL() {
    return UsuarioETL;
  }
  public void set_UsuarioETL(String UsuarioETL) {
    this.UsuarioETL = UsuarioETL;
  }
  public QueryResult with_UsuarioETL(String UsuarioETL) {
    this.UsuarioETL = UsuarioETL;
    return this;
  }
  private java.sql.Timestamp FechaCarga;
  public java.sql.Timestamp get_FechaCarga() {
    return FechaCarga;
  }
  public void set_FechaCarga(java.sql.Timestamp FechaCarga) {
    this.FechaCarga = FechaCarga;
  }
  public QueryResult with_FechaCarga(java.sql.Timestamp FechaCarga) {
    this.FechaCarga = FechaCarga;
    return this;
  }
  private java.sql.Timestamp FechaCambio;
  public java.sql.Timestamp get_FechaCambio() {
    return FechaCambio;
  }
  public void set_FechaCambio(java.sql.Timestamp FechaCambio) {
    this.FechaCambio = FechaCambio;
  }
  public QueryResult with_FechaCambio(java.sql.Timestamp FechaCambio) {
    this.FechaCambio = FechaCambio;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.TipoNomina_ID == null ? that.TipoNomina_ID == null : this.TipoNomina_ID.equals(that.TipoNomina_ID));
    equal = equal && (this.Empleado_ID == null ? that.Empleado_ID == null : this.Empleado_ID.equals(that.Empleado_ID));
    equal = equal && (this.FechaPago == null ? that.FechaPago == null : this.FechaPago.equals(that.FechaPago));
    equal = equal && (this.CuentaNatural_ID == null ? that.CuentaNatural_ID == null : this.CuentaNatural_ID.equals(that.CuentaNatural_ID));
    equal = equal && (this.AnalisisLocal_ID == null ? that.AnalisisLocal_ID == null : this.AnalisisLocal_ID.equals(that.AnalisisLocal_ID));
    equal = equal && (this.Concepto_ID == null ? that.Concepto_ID == null : this.Concepto_ID.equals(that.Concepto_ID));
    equal = equal && (this.Region_ID == null ? that.Region_ID == null : this.Region_ID.equals(that.Region_ID));
    equal = equal && (this.MontoPago == null ? that.MontoPago == null : this.MontoPago.equals(that.MontoPago));
    equal = equal && (this.TipoMoneda_ID == null ? that.TipoMoneda_ID == null : this.TipoMoneda_ID.equals(that.TipoMoneda_ID));
    equal = equal && (this.SistemaFuente == null ? that.SistemaFuente == null : this.SistemaFuente.equals(that.SistemaFuente));
    equal = equal && (this.UsuarioETL == null ? that.UsuarioETL == null : this.UsuarioETL.equals(that.UsuarioETL));
    equal = equal && (this.FechaCarga == null ? that.FechaCarga == null : this.FechaCarga.equals(that.FechaCarga));
    equal = equal && (this.FechaCambio == null ? that.FechaCambio == null : this.FechaCambio.equals(that.FechaCambio));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryResult)) {
      return false;
    }
    QueryResult that = (QueryResult) o;
    boolean equal = true;
    equal = equal && (this.TipoNomina_ID == null ? that.TipoNomina_ID == null : this.TipoNomina_ID.equals(that.TipoNomina_ID));
    equal = equal && (this.Empleado_ID == null ? that.Empleado_ID == null : this.Empleado_ID.equals(that.Empleado_ID));
    equal = equal && (this.FechaPago == null ? that.FechaPago == null : this.FechaPago.equals(that.FechaPago));
    equal = equal && (this.CuentaNatural_ID == null ? that.CuentaNatural_ID == null : this.CuentaNatural_ID.equals(that.CuentaNatural_ID));
    equal = equal && (this.AnalisisLocal_ID == null ? that.AnalisisLocal_ID == null : this.AnalisisLocal_ID.equals(that.AnalisisLocal_ID));
    equal = equal && (this.Concepto_ID == null ? that.Concepto_ID == null : this.Concepto_ID.equals(that.Concepto_ID));
    equal = equal && (this.Region_ID == null ? that.Region_ID == null : this.Region_ID.equals(that.Region_ID));
    equal = equal && (this.MontoPago == null ? that.MontoPago == null : this.MontoPago.equals(that.MontoPago));
    equal = equal && (this.TipoMoneda_ID == null ? that.TipoMoneda_ID == null : this.TipoMoneda_ID.equals(that.TipoMoneda_ID));
    equal = equal && (this.SistemaFuente == null ? that.SistemaFuente == null : this.SistemaFuente.equals(that.SistemaFuente));
    equal = equal && (this.UsuarioETL == null ? that.UsuarioETL == null : this.UsuarioETL.equals(that.UsuarioETL));
    equal = equal && (this.FechaCarga == null ? that.FechaCarga == null : this.FechaCarga.equals(that.FechaCarga));
    equal = equal && (this.FechaCambio == null ? that.FechaCambio == null : this.FechaCambio.equals(that.FechaCambio));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.TipoNomina_ID = JdbcWritableBridge.readString(1, __dbResults);
    this.Empleado_ID = JdbcWritableBridge.readString(2, __dbResults);
    this.FechaPago = JdbcWritableBridge.readDate(3, __dbResults);
    this.CuentaNatural_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.AnalisisLocal_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.Concepto_ID = JdbcWritableBridge.readInteger(6, __dbResults);
    this.Region_ID = JdbcWritableBridge.readString(7, __dbResults);
    this.MontoPago = JdbcWritableBridge.readDouble(8, __dbResults);
    this.TipoMoneda_ID = JdbcWritableBridge.readInteger(9, __dbResults);
    this.SistemaFuente = JdbcWritableBridge.readString(10, __dbResults);
    this.UsuarioETL = JdbcWritableBridge.readString(11, __dbResults);
    this.FechaCarga = JdbcWritableBridge.readTimestamp(12, __dbResults);
    this.FechaCambio = JdbcWritableBridge.readTimestamp(13, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.TipoNomina_ID = JdbcWritableBridge.readString(1, __dbResults);
    this.Empleado_ID = JdbcWritableBridge.readString(2, __dbResults);
    this.FechaPago = JdbcWritableBridge.readDate(3, __dbResults);
    this.CuentaNatural_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.AnalisisLocal_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.Concepto_ID = JdbcWritableBridge.readInteger(6, __dbResults);
    this.Region_ID = JdbcWritableBridge.readString(7, __dbResults);
    this.MontoPago = JdbcWritableBridge.readDouble(8, __dbResults);
    this.TipoMoneda_ID = JdbcWritableBridge.readInteger(9, __dbResults);
    this.SistemaFuente = JdbcWritableBridge.readString(10, __dbResults);
    this.UsuarioETL = JdbcWritableBridge.readString(11, __dbResults);
    this.FechaCarga = JdbcWritableBridge.readTimestamp(12, __dbResults);
    this.FechaCambio = JdbcWritableBridge.readTimestamp(13, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(TipoNomina_ID, 1 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(Empleado_ID, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FechaPago, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(CuentaNatural_ID, 4 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AnalisisLocal_ID, 5 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeInteger(Concepto_ID, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(Region_ID, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(MontoPago, 8 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeInteger(TipoMoneda_ID, 9 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(SistemaFuente, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(UsuarioETL, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(FechaCarga, 12 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(FechaCambio, 13 + __off, 93, __dbStmt);
    return 13;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(TipoNomina_ID, 1 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(Empleado_ID, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDate(FechaPago, 3 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeString(CuentaNatural_ID, 4 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeString(AnalisisLocal_ID, 5 + __off, 1, __dbStmt);
    JdbcWritableBridge.writeInteger(Concepto_ID, 6 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(Region_ID, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeDouble(MontoPago, 8 + __off, 6, __dbStmt);
    JdbcWritableBridge.writeInteger(TipoMoneda_ID, 9 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeString(SistemaFuente, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(UsuarioETL, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(FechaCarga, 12 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(FechaCambio, 13 + __off, 93, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.TipoNomina_ID = null;
    } else {
    this.TipoNomina_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Empleado_ID = null;
    } else {
    this.Empleado_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FechaPago = null;
    } else {
    this.FechaPago = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.CuentaNatural_ID = null;
    } else {
    this.CuentaNatural_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.AnalisisLocal_ID = null;
    } else {
    this.AnalisisLocal_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Concepto_ID = null;
    } else {
    this.Concepto_ID = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.Region_ID = null;
    } else {
    this.Region_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.MontoPago = null;
    } else {
    this.MontoPago = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.TipoMoneda_ID = null;
    } else {
    this.TipoMoneda_ID = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.SistemaFuente = null;
    } else {
    this.SistemaFuente = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.UsuarioETL = null;
    } else {
    this.UsuarioETL = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.FechaCarga = null;
    } else {
    this.FechaCarga = new Timestamp(__dataIn.readLong());
    this.FechaCarga.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.FechaCambio = null;
    } else {
    this.FechaCambio = new Timestamp(__dataIn.readLong());
    this.FechaCambio.setNanos(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.TipoNomina_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TipoNomina_ID);
    }
    if (null == this.Empleado_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Empleado_ID);
    }
    if (null == this.FechaPago) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FechaPago.getTime());
    }
    if (null == this.CuentaNatural_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CuentaNatural_ID);
    }
    if (null == this.AnalisisLocal_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AnalisisLocal_ID);
    }
    if (null == this.Concepto_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Concepto_ID);
    }
    if (null == this.Region_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Region_ID);
    }
    if (null == this.MontoPago) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.MontoPago);
    }
    if (null == this.TipoMoneda_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.TipoMoneda_ID);
    }
    if (null == this.SistemaFuente) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SistemaFuente);
    }
    if (null == this.UsuarioETL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, UsuarioETL);
    }
    if (null == this.FechaCarga) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FechaCarga.getTime());
    __dataOut.writeInt(this.FechaCarga.getNanos());
    }
    if (null == this.FechaCambio) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FechaCambio.getTime());
    __dataOut.writeInt(this.FechaCambio.getNanos());
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.TipoNomina_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TipoNomina_ID);
    }
    if (null == this.Empleado_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Empleado_ID);
    }
    if (null == this.FechaPago) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FechaPago.getTime());
    }
    if (null == this.CuentaNatural_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, CuentaNatural_ID);
    }
    if (null == this.AnalisisLocal_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, AnalisisLocal_ID);
    }
    if (null == this.Concepto_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.Concepto_ID);
    }
    if (null == this.Region_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Region_ID);
    }
    if (null == this.MontoPago) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.MontoPago);
    }
    if (null == this.TipoMoneda_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.TipoMoneda_ID);
    }
    if (null == this.SistemaFuente) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, SistemaFuente);
    }
    if (null == this.UsuarioETL) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, UsuarioETL);
    }
    if (null == this.FechaCarga) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FechaCarga.getTime());
    __dataOut.writeInt(this.FechaCarga.getNanos());
    }
    if (null == this.FechaCambio) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.FechaCambio.getTime());
    __dataOut.writeInt(this.FechaCambio.getNanos());
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(TipoNomina_ID==null?"null":TipoNomina_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(Empleado_ID==null?"null":Empleado_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FechaPago==null?"null":"" + FechaPago, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(CuentaNatural_ID==null?"null":CuentaNatural_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(AnalisisLocal_ID==null?"null":AnalisisLocal_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Concepto_ID==null?"null":"" + Concepto_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(Region_ID==null?"null":Region_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MontoPago==null?"null":"" + MontoPago, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TipoMoneda_ID==null?"null":"" + TipoMoneda_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(SistemaFuente==null?"null":SistemaFuente, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(UsuarioETL==null?"null":UsuarioETL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FechaCarga==null?"null":"" + FechaCarga, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FechaCambio==null?"null":"" + FechaCambio, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(TipoNomina_ID==null?"null":TipoNomina_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(Empleado_ID==null?"null":Empleado_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FechaPago==null?"null":"" + FechaPago, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(CuentaNatural_ID==null?"null":CuentaNatural_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(AnalisisLocal_ID==null?"null":AnalisisLocal_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Concepto_ID==null?"null":"" + Concepto_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(Region_ID==null?"null":Region_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(MontoPago==null?"null":"" + MontoPago, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TipoMoneda_ID==null?"null":"" + TipoMoneda_ID, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(SistemaFuente==null?"null":SistemaFuente, delimiters));
    __sb.append(fieldDelim);
    // special case for strings hive, droppingdelimiters \n,\r,\01 from strings
    __sb.append(FieldFormatter.hiveStringDropDelims(UsuarioETL==null?"null":UsuarioETL, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FechaCarga==null?"null":"" + FechaCarga, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(FechaCambio==null?"null":"" + FechaCambio, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 1, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.TipoNomina_ID = null; } else {
      this.TipoNomina_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Empleado_ID = null; } else {
      this.Empleado_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.FechaPago = null; } else {
      this.FechaPago = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.CuentaNatural_ID = null; } else {
      this.CuentaNatural_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.AnalisisLocal_ID = null; } else {
      this.AnalisisLocal_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.Concepto_ID = null; } else {
      this.Concepto_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Region_ID = null; } else {
      this.Region_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.MontoPago = null; } else {
      this.MontoPago = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.TipoMoneda_ID = null; } else {
      this.TipoMoneda_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.SistemaFuente = null; } else {
      this.SistemaFuente = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.UsuarioETL = null; } else {
      this.UsuarioETL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.FechaCarga = null; } else {
      this.FechaCarga = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.FechaCambio = null; } else {
      this.FechaCambio = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.TipoNomina_ID = null; } else {
      this.TipoNomina_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Empleado_ID = null; } else {
      this.Empleado_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.FechaPago = null; } else {
      this.FechaPago = java.sql.Date.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.CuentaNatural_ID = null; } else {
      this.CuentaNatural_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.AnalisisLocal_ID = null; } else {
      this.AnalisisLocal_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.Concepto_ID = null; } else {
      this.Concepto_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Region_ID = null; } else {
      this.Region_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.MontoPago = null; } else {
      this.MontoPago = Double.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.TipoMoneda_ID = null; } else {
      this.TipoMoneda_ID = Integer.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.SistemaFuente = null; } else {
      this.SistemaFuente = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.UsuarioETL = null; } else {
      this.UsuarioETL = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.FechaCarga = null; } else {
      this.FechaCarga = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N") || __cur_str.length() == 0) { this.FechaCambio = null; } else {
      this.FechaCambio = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    QueryResult o = (QueryResult) super.clone();
    o.FechaPago = (o.FechaPago != null) ? (java.sql.Date) o.FechaPago.clone() : null;
    o.FechaCarga = (o.FechaCarga != null) ? (java.sql.Timestamp) o.FechaCarga.clone() : null;
    o.FechaCambio = (o.FechaCambio != null) ? (java.sql.Timestamp) o.FechaCambio.clone() : null;
    return o;
  }

  public void clone0(QueryResult o) throws CloneNotSupportedException {
    o.FechaPago = (o.FechaPago != null) ? (java.sql.Date) o.FechaPago.clone() : null;
    o.FechaCarga = (o.FechaCarga != null) ? (java.sql.Timestamp) o.FechaCarga.clone() : null;
    o.FechaCambio = (o.FechaCambio != null) ? (java.sql.Timestamp) o.FechaCambio.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("TipoNomina_ID", this.TipoNomina_ID);
    __sqoop$field_map.put("Empleado_ID", this.Empleado_ID);
    __sqoop$field_map.put("FechaPago", this.FechaPago);
    __sqoop$field_map.put("CuentaNatural_ID", this.CuentaNatural_ID);
    __sqoop$field_map.put("AnalisisLocal_ID", this.AnalisisLocal_ID);
    __sqoop$field_map.put("Concepto_ID", this.Concepto_ID);
    __sqoop$field_map.put("Region_ID", this.Region_ID);
    __sqoop$field_map.put("MontoPago", this.MontoPago);
    __sqoop$field_map.put("TipoMoneda_ID", this.TipoMoneda_ID);
    __sqoop$field_map.put("SistemaFuente", this.SistemaFuente);
    __sqoop$field_map.put("UsuarioETL", this.UsuarioETL);
    __sqoop$field_map.put("FechaCarga", this.FechaCarga);
    __sqoop$field_map.put("FechaCambio", this.FechaCambio);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("TipoNomina_ID", this.TipoNomina_ID);
    __sqoop$field_map.put("Empleado_ID", this.Empleado_ID);
    __sqoop$field_map.put("FechaPago", this.FechaPago);
    __sqoop$field_map.put("CuentaNatural_ID", this.CuentaNatural_ID);
    __sqoop$field_map.put("AnalisisLocal_ID", this.AnalisisLocal_ID);
    __sqoop$field_map.put("Concepto_ID", this.Concepto_ID);
    __sqoop$field_map.put("Region_ID", this.Region_ID);
    __sqoop$field_map.put("MontoPago", this.MontoPago);
    __sqoop$field_map.put("TipoMoneda_ID", this.TipoMoneda_ID);
    __sqoop$field_map.put("SistemaFuente", this.SistemaFuente);
    __sqoop$field_map.put("UsuarioETL", this.UsuarioETL);
    __sqoop$field_map.put("FechaCarga", this.FechaCarga);
    __sqoop$field_map.put("FechaCambio", this.FechaCambio);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
