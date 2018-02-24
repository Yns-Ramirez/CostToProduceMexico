// ORM class for table 'cp_data_sumario'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Jul 26 09:36:56 CDT 2017
// For connector: org.apache.sqoop.manager.OracleManager
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

public class cp_data_sumario extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("Periodo", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Periodo = (String)value;
      }
    });
    setters.put("execution_date", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        execution_date = (String)value;
      }
    });
    setters.put("Organizacion_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Organizacion_ID = (String)value;
      }
    });
    setters.put("Pais_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Pais_ID = (String)value;
      }
    });
    setters.put("EntidadLegal_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        EntidadLegal_ID = (String)value;
      }
    });
    setters.put("Planta_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Planta_ID = (String)value;
      }
    });
    setters.put("Lineas_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Lineas_ID = (String)value;
      }
    });
    setters.put("Turno_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Turno_ID = (String)value;
      }
    });
    setters.put("Producto_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Producto_ID = (String)value;
      }
    });
    setters.put("Rubro_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Rubro_ID = (String)value;
      }
    });
    setters.put("Tipo_costo", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Tipo_costo = (String)value;
      }
    });
    setters.put("Value", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Value = (String)value;
      }
    });
    setters.put("TMoneda_ID", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        TMoneda_ID = (String)value;
      }
    });
  }
  public cp_data_sumario() {
    init0();
  }
  private String Periodo;
  public String get_Periodo() {
    return Periodo;
  }
  public void set_Periodo(String Periodo) {
    this.Periodo = Periodo;
  }
  public cp_data_sumario with_Periodo(String Periodo) {
    this.Periodo = Periodo;
    return this;
  }
  private String execution_date;
  public String get_execution_date() {
    return execution_date;
  }
  public void set_execution_date(String execution_date) {
    this.execution_date = execution_date;
  }
  public cp_data_sumario with_execution_date(String execution_date) {
    this.execution_date = execution_date;
    return this;
  }
  private String Organizacion_ID;
  public String get_Organizacion_ID() {
    return Organizacion_ID;
  }
  public void set_Organizacion_ID(String Organizacion_ID) {
    this.Organizacion_ID = Organizacion_ID;
  }
  public cp_data_sumario with_Organizacion_ID(String Organizacion_ID) {
    this.Organizacion_ID = Organizacion_ID;
    return this;
  }
  private String Pais_ID;
  public String get_Pais_ID() {
    return Pais_ID;
  }
  public void set_Pais_ID(String Pais_ID) {
    this.Pais_ID = Pais_ID;
  }
  public cp_data_sumario with_Pais_ID(String Pais_ID) {
    this.Pais_ID = Pais_ID;
    return this;
  }
  private String EntidadLegal_ID;
  public String get_EntidadLegal_ID() {
    return EntidadLegal_ID;
  }
  public void set_EntidadLegal_ID(String EntidadLegal_ID) {
    this.EntidadLegal_ID = EntidadLegal_ID;
  }
  public cp_data_sumario with_EntidadLegal_ID(String EntidadLegal_ID) {
    this.EntidadLegal_ID = EntidadLegal_ID;
    return this;
  }
  private String Planta_ID;
  public String get_Planta_ID() {
    return Planta_ID;
  }
  public void set_Planta_ID(String Planta_ID) {
    this.Planta_ID = Planta_ID;
  }
  public cp_data_sumario with_Planta_ID(String Planta_ID) {
    this.Planta_ID = Planta_ID;
    return this;
  }
  private String Lineas_ID;
  public String get_Lineas_ID() {
    return Lineas_ID;
  }
  public void set_Lineas_ID(String Lineas_ID) {
    this.Lineas_ID = Lineas_ID;
  }
  public cp_data_sumario with_Lineas_ID(String Lineas_ID) {
    this.Lineas_ID = Lineas_ID;
    return this;
  }
  private String Turno_ID;
  public String get_Turno_ID() {
    return Turno_ID;
  }
  public void set_Turno_ID(String Turno_ID) {
    this.Turno_ID = Turno_ID;
  }
  public cp_data_sumario with_Turno_ID(String Turno_ID) {
    this.Turno_ID = Turno_ID;
    return this;
  }
  private String Producto_ID;
  public String get_Producto_ID() {
    return Producto_ID;
  }
  public void set_Producto_ID(String Producto_ID) {
    this.Producto_ID = Producto_ID;
  }
  public cp_data_sumario with_Producto_ID(String Producto_ID) {
    this.Producto_ID = Producto_ID;
    return this;
  }
  private String Rubro_ID;
  public String get_Rubro_ID() {
    return Rubro_ID;
  }
  public void set_Rubro_ID(String Rubro_ID) {
    this.Rubro_ID = Rubro_ID;
  }
  public cp_data_sumario with_Rubro_ID(String Rubro_ID) {
    this.Rubro_ID = Rubro_ID;
    return this;
  }
  private String Tipo_costo;
  public String get_Tipo_costo() {
    return Tipo_costo;
  }
  public void set_Tipo_costo(String Tipo_costo) {
    this.Tipo_costo = Tipo_costo;
  }
  public cp_data_sumario with_Tipo_costo(String Tipo_costo) {
    this.Tipo_costo = Tipo_costo;
    return this;
  }
  private String Value;
  public String get_Value() {
    return Value;
  }
  public void set_Value(String Value) {
    this.Value = Value;
  }
  public cp_data_sumario with_Value(String Value) {
    this.Value = Value;
    return this;
  }
  private String TMoneda_ID;
  public String get_TMoneda_ID() {
    return TMoneda_ID;
  }
  public void set_TMoneda_ID(String TMoneda_ID) {
    this.TMoneda_ID = TMoneda_ID;
  }
  public cp_data_sumario with_TMoneda_ID(String TMoneda_ID) {
    this.TMoneda_ID = TMoneda_ID;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cp_data_sumario)) {
      return false;
    }
    cp_data_sumario that = (cp_data_sumario) o;
    boolean equal = true;
    equal = equal && (this.Periodo == null ? that.Periodo == null : this.Periodo.equals(that.Periodo));
    equal = equal && (this.execution_date == null ? that.execution_date == null : this.execution_date.equals(that.execution_date));
    equal = equal && (this.Organizacion_ID == null ? that.Organizacion_ID == null : this.Organizacion_ID.equals(that.Organizacion_ID));
    equal = equal && (this.Pais_ID == null ? that.Pais_ID == null : this.Pais_ID.equals(that.Pais_ID));
    equal = equal && (this.EntidadLegal_ID == null ? that.EntidadLegal_ID == null : this.EntidadLegal_ID.equals(that.EntidadLegal_ID));
    equal = equal && (this.Planta_ID == null ? that.Planta_ID == null : this.Planta_ID.equals(that.Planta_ID));
    equal = equal && (this.Lineas_ID == null ? that.Lineas_ID == null : this.Lineas_ID.equals(that.Lineas_ID));
    equal = equal && (this.Turno_ID == null ? that.Turno_ID == null : this.Turno_ID.equals(that.Turno_ID));
    equal = equal && (this.Producto_ID == null ? that.Producto_ID == null : this.Producto_ID.equals(that.Producto_ID));
    equal = equal && (this.Rubro_ID == null ? that.Rubro_ID == null : this.Rubro_ID.equals(that.Rubro_ID));
    equal = equal && (this.Tipo_costo == null ? that.Tipo_costo == null : this.Tipo_costo.equals(that.Tipo_costo));
    equal = equal && (this.Value == null ? that.Value == null : this.Value.equals(that.Value));
    equal = equal && (this.TMoneda_ID == null ? that.TMoneda_ID == null : this.TMoneda_ID.equals(that.TMoneda_ID));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cp_data_sumario)) {
      return false;
    }
    cp_data_sumario that = (cp_data_sumario) o;
    boolean equal = true;
    equal = equal && (this.Periodo == null ? that.Periodo == null : this.Periodo.equals(that.Periodo));
    equal = equal && (this.execution_date == null ? that.execution_date == null : this.execution_date.equals(that.execution_date));
    equal = equal && (this.Organizacion_ID == null ? that.Organizacion_ID == null : this.Organizacion_ID.equals(that.Organizacion_ID));
    equal = equal && (this.Pais_ID == null ? that.Pais_ID == null : this.Pais_ID.equals(that.Pais_ID));
    equal = equal && (this.EntidadLegal_ID == null ? that.EntidadLegal_ID == null : this.EntidadLegal_ID.equals(that.EntidadLegal_ID));
    equal = equal && (this.Planta_ID == null ? that.Planta_ID == null : this.Planta_ID.equals(that.Planta_ID));
    equal = equal && (this.Lineas_ID == null ? that.Lineas_ID == null : this.Lineas_ID.equals(that.Lineas_ID));
    equal = equal && (this.Turno_ID == null ? that.Turno_ID == null : this.Turno_ID.equals(that.Turno_ID));
    equal = equal && (this.Producto_ID == null ? that.Producto_ID == null : this.Producto_ID.equals(that.Producto_ID));
    equal = equal && (this.Rubro_ID == null ? that.Rubro_ID == null : this.Rubro_ID.equals(that.Rubro_ID));
    equal = equal && (this.Tipo_costo == null ? that.Tipo_costo == null : this.Tipo_costo.equals(that.Tipo_costo));
    equal = equal && (this.Value == null ? that.Value == null : this.Value.equals(that.Value));
    equal = equal && (this.TMoneda_ID == null ? that.TMoneda_ID == null : this.TMoneda_ID.equals(that.TMoneda_ID));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.Periodo = JdbcWritableBridge.readString(1, __dbResults);
    this.execution_date = JdbcWritableBridge.readString(2, __dbResults);
    this.Organizacion_ID = JdbcWritableBridge.readString(3, __dbResults);
    this.Pais_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.EntidadLegal_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.Planta_ID = JdbcWritableBridge.readString(6, __dbResults);
    this.Lineas_ID = JdbcWritableBridge.readString(7, __dbResults);
    this.Turno_ID = JdbcWritableBridge.readString(8, __dbResults);
    this.Producto_ID = JdbcWritableBridge.readString(9, __dbResults);
    this.Rubro_ID = JdbcWritableBridge.readString(10, __dbResults);
    this.Tipo_costo = JdbcWritableBridge.readString(11, __dbResults);
    this.Value = JdbcWritableBridge.readString(12, __dbResults);
    this.TMoneda_ID = JdbcWritableBridge.readString(13, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.Periodo = JdbcWritableBridge.readString(1, __dbResults);
    this.execution_date = JdbcWritableBridge.readString(2, __dbResults);
    this.Organizacion_ID = JdbcWritableBridge.readString(3, __dbResults);
    this.Pais_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.EntidadLegal_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.Planta_ID = JdbcWritableBridge.readString(6, __dbResults);
    this.Lineas_ID = JdbcWritableBridge.readString(7, __dbResults);
    this.Turno_ID = JdbcWritableBridge.readString(8, __dbResults);
    this.Producto_ID = JdbcWritableBridge.readString(9, __dbResults);
    this.Rubro_ID = JdbcWritableBridge.readString(10, __dbResults);
    this.Tipo_costo = JdbcWritableBridge.readString(11, __dbResults);
    this.Value = JdbcWritableBridge.readString(12, __dbResults);
    this.TMoneda_ID = JdbcWritableBridge.readString(13, __dbResults);
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
    JdbcWritableBridge.writeString(Periodo, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(execution_date, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Organizacion_ID, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Pais_ID, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(EntidadLegal_ID, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Planta_ID, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Lineas_ID, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Turno_ID, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Producto_ID, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Rubro_ID, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Tipo_costo, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Value, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TMoneda_ID, 13 + __off, 12, __dbStmt);
    return 13;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(Periodo, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(execution_date, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Organizacion_ID, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Pais_ID, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(EntidadLegal_ID, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Planta_ID, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Lineas_ID, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Turno_ID, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Producto_ID, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Rubro_ID, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Tipo_costo, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Value, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(TMoneda_ID, 13 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.Periodo = null;
    } else {
    this.Periodo = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.execution_date = null;
    } else {
    this.execution_date = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Organizacion_ID = null;
    } else {
    this.Organizacion_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Pais_ID = null;
    } else {
    this.Pais_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.EntidadLegal_ID = null;
    } else {
    this.EntidadLegal_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Planta_ID = null;
    } else {
    this.Planta_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Lineas_ID = null;
    } else {
    this.Lineas_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Turno_ID = null;
    } else {
    this.Turno_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Producto_ID = null;
    } else {
    this.Producto_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Rubro_ID = null;
    } else {
    this.Rubro_ID = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Tipo_costo = null;
    } else {
    this.Tipo_costo = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Value = null;
    } else {
    this.Value = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.TMoneda_ID = null;
    } else {
    this.TMoneda_ID = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.Periodo) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Periodo);
    }
    if (null == this.execution_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, execution_date);
    }
    if (null == this.Organizacion_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Organizacion_ID);
    }
    if (null == this.Pais_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Pais_ID);
    }
    if (null == this.EntidadLegal_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EntidadLegal_ID);
    }
    if (null == this.Planta_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Planta_ID);
    }
    if (null == this.Lineas_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Lineas_ID);
    }
    if (null == this.Turno_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Turno_ID);
    }
    if (null == this.Producto_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Producto_ID);
    }
    if (null == this.Rubro_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Rubro_ID);
    }
    if (null == this.Tipo_costo) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Tipo_costo);
    }
    if (null == this.Value) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Value);
    }
    if (null == this.TMoneda_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TMoneda_ID);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.Periodo) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Periodo);
    }
    if (null == this.execution_date) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, execution_date);
    }
    if (null == this.Organizacion_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Organizacion_ID);
    }
    if (null == this.Pais_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Pais_ID);
    }
    if (null == this.EntidadLegal_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, EntidadLegal_ID);
    }
    if (null == this.Planta_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Planta_ID);
    }
    if (null == this.Lineas_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Lineas_ID);
    }
    if (null == this.Turno_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Turno_ID);
    }
    if (null == this.Producto_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Producto_ID);
    }
    if (null == this.Rubro_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Rubro_ID);
    }
    if (null == this.Tipo_costo) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Tipo_costo);
    }
    if (null == this.Value) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Value);
    }
    if (null == this.TMoneda_ID) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, TMoneda_ID);
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
    __sb.append(FieldFormatter.escapeAndEnclose(Periodo==null?"null":Periodo, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(execution_date==null?"null":execution_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Organizacion_ID==null?"null":Organizacion_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Pais_ID==null?"null":Pais_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EntidadLegal_ID==null?"null":EntidadLegal_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Planta_ID==null?"null":Planta_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Lineas_ID==null?"null":Lineas_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Turno_ID==null?"null":Turno_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Producto_ID==null?"null":Producto_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Rubro_ID==null?"null":Rubro_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Tipo_costo==null?"null":Tipo_costo, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Value==null?"null":Value, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TMoneda_ID==null?"null":TMoneda_ID, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(Periodo==null?"null":Periodo, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(execution_date==null?"null":execution_date, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Organizacion_ID==null?"null":Organizacion_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Pais_ID==null?"null":Pais_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(EntidadLegal_ID==null?"null":EntidadLegal_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Planta_ID==null?"null":Planta_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Lineas_ID==null?"null":Lineas_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Turno_ID==null?"null":Turno_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Producto_ID==null?"null":Producto_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Rubro_ID==null?"null":Rubro_ID, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Tipo_costo==null?"null":Tipo_costo, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Value==null?"null":Value, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(TMoneda_ID==null?"null":TMoneda_ID, delimiters));
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
    if (__cur_str.equals("\\N")) { this.Periodo = null; } else {
      this.Periodo = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.execution_date = null; } else {
      this.execution_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Organizacion_ID = null; } else {
      this.Organizacion_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Pais_ID = null; } else {
      this.Pais_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.EntidadLegal_ID = null; } else {
      this.EntidadLegal_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Planta_ID = null; } else {
      this.Planta_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Lineas_ID = null; } else {
      this.Lineas_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Turno_ID = null; } else {
      this.Turno_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Producto_ID = null; } else {
      this.Producto_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Rubro_ID = null; } else {
      this.Rubro_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Tipo_costo = null; } else {
      this.Tipo_costo = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Value = null; } else {
      this.Value = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.TMoneda_ID = null; } else {
      this.TMoneda_ID = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Periodo = null; } else {
      this.Periodo = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.execution_date = null; } else {
      this.execution_date = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Organizacion_ID = null; } else {
      this.Organizacion_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Pais_ID = null; } else {
      this.Pais_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.EntidadLegal_ID = null; } else {
      this.EntidadLegal_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Planta_ID = null; } else {
      this.Planta_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Lineas_ID = null; } else {
      this.Lineas_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Turno_ID = null; } else {
      this.Turno_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Producto_ID = null; } else {
      this.Producto_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Rubro_ID = null; } else {
      this.Rubro_ID = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Tipo_costo = null; } else {
      this.Tipo_costo = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Value = null; } else {
      this.Value = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.TMoneda_ID = null; } else {
      this.TMoneda_ID = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    cp_data_sumario o = (cp_data_sumario) super.clone();
    return o;
  }

  public void clone0(cp_data_sumario o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("Periodo", this.Periodo);
    __sqoop$field_map.put("execution_date", this.execution_date);
    __sqoop$field_map.put("Organizacion_ID", this.Organizacion_ID);
    __sqoop$field_map.put("Pais_ID", this.Pais_ID);
    __sqoop$field_map.put("EntidadLegal_ID", this.EntidadLegal_ID);
    __sqoop$field_map.put("Planta_ID", this.Planta_ID);
    __sqoop$field_map.put("Lineas_ID", this.Lineas_ID);
    __sqoop$field_map.put("Turno_ID", this.Turno_ID);
    __sqoop$field_map.put("Producto_ID", this.Producto_ID);
    __sqoop$field_map.put("Rubro_ID", this.Rubro_ID);
    __sqoop$field_map.put("Tipo_costo", this.Tipo_costo);
    __sqoop$field_map.put("Value", this.Value);
    __sqoop$field_map.put("TMoneda_ID", this.TMoneda_ID);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("Periodo", this.Periodo);
    __sqoop$field_map.put("execution_date", this.execution_date);
    __sqoop$field_map.put("Organizacion_ID", this.Organizacion_ID);
    __sqoop$field_map.put("Pais_ID", this.Pais_ID);
    __sqoop$field_map.put("EntidadLegal_ID", this.EntidadLegal_ID);
    __sqoop$field_map.put("Planta_ID", this.Planta_ID);
    __sqoop$field_map.put("Lineas_ID", this.Lineas_ID);
    __sqoop$field_map.put("Turno_ID", this.Turno_ID);
    __sqoop$field_map.put("Producto_ID", this.Producto_ID);
    __sqoop$field_map.put("Rubro_ID", this.Rubro_ID);
    __sqoop$field_map.put("Tipo_costo", this.Tipo_costo);
    __sqoop$field_map.put("Value", this.Value);
    __sqoop$field_map.put("TMoneda_ID", this.TMoneda_ID);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
