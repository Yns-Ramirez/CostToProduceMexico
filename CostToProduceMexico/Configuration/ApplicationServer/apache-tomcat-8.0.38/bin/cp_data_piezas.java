// ORM class for table 'cp_data_piezas'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Wed Jul 26 09:29:37 CDT 2017
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

public class cp_data_piezas extends SqoopRecord  implements DBWritable, Writable {
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
    setters.put("Concepto", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Concepto = (String)value;
      }
    });
    setters.put("Cantidad", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        Cantidad = (String)value;
      }
    });
  }
  public cp_data_piezas() {
    init0();
  }
  private String Periodo;
  public String get_Periodo() {
    return Periodo;
  }
  public void set_Periodo(String Periodo) {
    this.Periodo = Periodo;
  }
  public cp_data_piezas with_Periodo(String Periodo) {
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
  public cp_data_piezas with_execution_date(String execution_date) {
    this.execution_date = execution_date;
    return this;
  }
  private String EntidadLegal_ID;
  public String get_EntidadLegal_ID() {
    return EntidadLegal_ID;
  }
  public void set_EntidadLegal_ID(String EntidadLegal_ID) {
    this.EntidadLegal_ID = EntidadLegal_ID;
  }
  public cp_data_piezas with_EntidadLegal_ID(String EntidadLegal_ID) {
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
  public cp_data_piezas with_Planta_ID(String Planta_ID) {
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
  public cp_data_piezas with_Lineas_ID(String Lineas_ID) {
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
  public cp_data_piezas with_Turno_ID(String Turno_ID) {
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
  public cp_data_piezas with_Producto_ID(String Producto_ID) {
    this.Producto_ID = Producto_ID;
    return this;
  }
  private String Concepto;
  public String get_Concepto() {
    return Concepto;
  }
  public void set_Concepto(String Concepto) {
    this.Concepto = Concepto;
  }
  public cp_data_piezas with_Concepto(String Concepto) {
    this.Concepto = Concepto;
    return this;
  }
  private String Cantidad;
  public String get_Cantidad() {
    return Cantidad;
  }
  public void set_Cantidad(String Cantidad) {
    this.Cantidad = Cantidad;
  }
  public cp_data_piezas with_Cantidad(String Cantidad) {
    this.Cantidad = Cantidad;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cp_data_piezas)) {
      return false;
    }
    cp_data_piezas that = (cp_data_piezas) o;
    boolean equal = true;
    equal = equal && (this.Periodo == null ? that.Periodo == null : this.Periodo.equals(that.Periodo));
    equal = equal && (this.execution_date == null ? that.execution_date == null : this.execution_date.equals(that.execution_date));
    equal = equal && (this.EntidadLegal_ID == null ? that.EntidadLegal_ID == null : this.EntidadLegal_ID.equals(that.EntidadLegal_ID));
    equal = equal && (this.Planta_ID == null ? that.Planta_ID == null : this.Planta_ID.equals(that.Planta_ID));
    equal = equal && (this.Lineas_ID == null ? that.Lineas_ID == null : this.Lineas_ID.equals(that.Lineas_ID));
    equal = equal && (this.Turno_ID == null ? that.Turno_ID == null : this.Turno_ID.equals(that.Turno_ID));
    equal = equal && (this.Producto_ID == null ? that.Producto_ID == null : this.Producto_ID.equals(that.Producto_ID));
    equal = equal && (this.Concepto == null ? that.Concepto == null : this.Concepto.equals(that.Concepto));
    equal = equal && (this.Cantidad == null ? that.Cantidad == null : this.Cantidad.equals(that.Cantidad));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof cp_data_piezas)) {
      return false;
    }
    cp_data_piezas that = (cp_data_piezas) o;
    boolean equal = true;
    equal = equal && (this.Periodo == null ? that.Periodo == null : this.Periodo.equals(that.Periodo));
    equal = equal && (this.execution_date == null ? that.execution_date == null : this.execution_date.equals(that.execution_date));
    equal = equal && (this.EntidadLegal_ID == null ? that.EntidadLegal_ID == null : this.EntidadLegal_ID.equals(that.EntidadLegal_ID));
    equal = equal && (this.Planta_ID == null ? that.Planta_ID == null : this.Planta_ID.equals(that.Planta_ID));
    equal = equal && (this.Lineas_ID == null ? that.Lineas_ID == null : this.Lineas_ID.equals(that.Lineas_ID));
    equal = equal && (this.Turno_ID == null ? that.Turno_ID == null : this.Turno_ID.equals(that.Turno_ID));
    equal = equal && (this.Producto_ID == null ? that.Producto_ID == null : this.Producto_ID.equals(that.Producto_ID));
    equal = equal && (this.Concepto == null ? that.Concepto == null : this.Concepto.equals(that.Concepto));
    equal = equal && (this.Cantidad == null ? that.Cantidad == null : this.Cantidad.equals(that.Cantidad));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.Periodo = JdbcWritableBridge.readString(1, __dbResults);
    this.execution_date = JdbcWritableBridge.readString(2, __dbResults);
    this.EntidadLegal_ID = JdbcWritableBridge.readString(3, __dbResults);
    this.Planta_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.Lineas_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.Turno_ID = JdbcWritableBridge.readString(6, __dbResults);
    this.Producto_ID = JdbcWritableBridge.readString(7, __dbResults);
    this.Concepto = JdbcWritableBridge.readString(8, __dbResults);
    this.Cantidad = JdbcWritableBridge.readString(9, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.Periodo = JdbcWritableBridge.readString(1, __dbResults);
    this.execution_date = JdbcWritableBridge.readString(2, __dbResults);
    this.EntidadLegal_ID = JdbcWritableBridge.readString(3, __dbResults);
    this.Planta_ID = JdbcWritableBridge.readString(4, __dbResults);
    this.Lineas_ID = JdbcWritableBridge.readString(5, __dbResults);
    this.Turno_ID = JdbcWritableBridge.readString(6, __dbResults);
    this.Producto_ID = JdbcWritableBridge.readString(7, __dbResults);
    this.Concepto = JdbcWritableBridge.readString(8, __dbResults);
    this.Cantidad = JdbcWritableBridge.readString(9, __dbResults);
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
    JdbcWritableBridge.writeString(EntidadLegal_ID, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Planta_ID, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Lineas_ID, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Turno_ID, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Producto_ID, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Concepto, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Cantidad, 9 + __off, 12, __dbStmt);
    return 9;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(Periodo, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(execution_date, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(EntidadLegal_ID, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Planta_ID, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Lineas_ID, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Turno_ID, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Producto_ID, 7 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Concepto, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(Cantidad, 9 + __off, 12, __dbStmt);
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
        this.Concepto = null;
    } else {
    this.Concepto = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.Cantidad = null;
    } else {
    this.Cantidad = Text.readString(__dataIn);
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
    if (null == this.Concepto) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Concepto);
    }
    if (null == this.Cantidad) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Cantidad);
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
    if (null == this.Concepto) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Concepto);
    }
    if (null == this.Cantidad) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, Cantidad);
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
    __sb.append(FieldFormatter.escapeAndEnclose(Concepto==null?"null":Concepto, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Cantidad==null?"null":Cantidad, delimiters));
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
    __sb.append(FieldFormatter.escapeAndEnclose(Concepto==null?"null":Concepto, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(Cantidad==null?"null":Cantidad, delimiters));
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
    if (__cur_str.equals("\\N")) { this.Concepto = null; } else {
      this.Concepto = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Cantidad = null; } else {
      this.Cantidad = __cur_str;
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
    if (__cur_str.equals("\\N")) { this.Concepto = null; } else {
      this.Concepto = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("\\N")) { this.Cantidad = null; } else {
      this.Cantidad = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    cp_data_piezas o = (cp_data_piezas) super.clone();
    return o;
  }

  public void clone0(cp_data_piezas o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("Periodo", this.Periodo);
    __sqoop$field_map.put("execution_date", this.execution_date);
    __sqoop$field_map.put("EntidadLegal_ID", this.EntidadLegal_ID);
    __sqoop$field_map.put("Planta_ID", this.Planta_ID);
    __sqoop$field_map.put("Lineas_ID", this.Lineas_ID);
    __sqoop$field_map.put("Turno_ID", this.Turno_ID);
    __sqoop$field_map.put("Producto_ID", this.Producto_ID);
    __sqoop$field_map.put("Concepto", this.Concepto);
    __sqoop$field_map.put("Cantidad", this.Cantidad);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("Periodo", this.Periodo);
    __sqoop$field_map.put("execution_date", this.execution_date);
    __sqoop$field_map.put("EntidadLegal_ID", this.EntidadLegal_ID);
    __sqoop$field_map.put("Planta_ID", this.Planta_ID);
    __sqoop$field_map.put("Lineas_ID", this.Lineas_ID);
    __sqoop$field_map.put("Turno_ID", this.Turno_ID);
    __sqoop$field_map.put("Producto_ID", this.Producto_ID);
    __sqoop$field_map.put("Concepto", this.Concepto);
    __sqoop$field_map.put("Cantidad", this.Cantidad);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
