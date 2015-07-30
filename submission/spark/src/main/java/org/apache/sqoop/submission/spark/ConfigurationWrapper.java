package org.apache.sqoop.submission.spark;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ConfigurationWrapper implements Serializable {
  private transient Configuration conf;

  public ConfigurationWrapper(Configuration conf) {
    this.conf = conf;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    conf.write(out);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    conf.readFields(in);
  }

  public Confi getConfiguration()
}
