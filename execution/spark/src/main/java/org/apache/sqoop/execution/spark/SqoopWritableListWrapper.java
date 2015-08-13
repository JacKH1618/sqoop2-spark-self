package org.apache.sqoop.execution.spark;


import org.apache.sqoop.mapredsparkcommon.SqoopWritable;

import java.util.LinkedList;


// jackh: Maybe improve this later on so that the list is created and maintained internally
// in the class (add method, etc) instead of the user having to create a list and pass it

public class SqoopWritableListWrapper {

  private LinkedList<SqoopWritable> sqoopWritables;

  public SqoopWritableListWrapper(LinkedList<SqoopWritable> sqoopWritables) {
    this.sqoopWritables = sqoopWritables;
  }

  public LinkedList<SqoopWritable> getSqoopWritablesList() {
   return sqoopWritables;
  }

}
