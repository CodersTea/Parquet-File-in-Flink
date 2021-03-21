package com.coderstea.bigdata;

import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class WriteParquetFileWithFlinkDataSet {
  private static final String OUTPUT_FILE_PATH = "/home/coderstea/bigdata/flink-parquet-file/";

  public static void main(String[] args) throws Exception {

    InputStream employeeSchemaFileStream = WriteParquetFileWithFlinkDataSet.class.getClassLoader().getResourceAsStream("employeeRecord.avsc");
    Schema schema = new Schema.Parser().parse(employeeSchemaFileStream);

    Gson gson = new Gson();
    List<GenericRecord> collect = IntStream.range(0, 100)
            .mapToObj(x -> new EmployeeRecord(x,
                    "firstname" + x,
                    "lastName" + x,
                    "phone" + x,
                    "email@" + x,
                    new String[]{"java", "flink"}))
            .map(gson::toJson)
            .map(json -> getGenericRecordFromJson(schema, json))
            .collect(Collectors.toList());

    HadoopOutputFormat<Void, GenericRecord> parquetFormat = getOutputFormat(schema);

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.fromCollection(collect)
  .map(record -> new Tuple2<Void, GenericRecord>(null, record))
  .output(parquetFormat);
  }

  private static GenericRecord getGenericRecordFromJson(Schema schema, String employeeJson) {
    try {
      DecoderFactory decoderFactory = new DecoderFactory();
      Decoder decoder = decoderFactory.jsonDecoder(schema, employeeJson);
      DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
      GenericRecord record = reader.read(null, decoder);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  private static HadoopOutputFormat<Void, GenericRecord> getOutputFormat(Schema schema) throws IOException {
    // Avro File output setup
    Job job = Job.getInstance();
    FileOutputFormat.setOutputPath(job, new Path(OUTPUT_FILE_PATH));
    AvroParquetOutputFormat.setSchema(job, schema);
    ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);

    // Wrapping AvroOutput in HadoopOutputFormat to pass it to the DataSet API.
    return new HadoopOutputFormat<>(new AvroParquetOutputFormat<>(), job);
  }
}

class EmployeeRecord {
  private int id;
  private String firstName, lastName, phoneNumber, emailAddress;
  private String[] skills;

  public EmployeeRecord() {
  }

  public EmployeeRecord(int id, String firstName, String lastName, String phoneNumber, String emailAddress, String[] skills) {
    this.id = id;
    this.firstName = firstName;
    this.lastName = lastName;
    this.phoneNumber = phoneNumber;
    this.emailAddress = emailAddress;
    this.skills = skills;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getPhoneNumber() {
    return phoneNumber;
  }

  public void setPhoneNumber(String phoneNumber) {
    this.phoneNumber = phoneNumber;
  }

  public String getEmailAddress() {
    return emailAddress;
  }

  public void setEmailAddress(String emailAddress) {
    this.emailAddress = emailAddress;
  }

  public String[] getSkills() {
    return skills;
  }

  public void setSkills(String[] skills) {
    this.skills = skills;
  }
}