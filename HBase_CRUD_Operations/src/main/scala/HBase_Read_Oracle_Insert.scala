//import net.liftweb.json.{DefaultFormats, JArray, parse}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by cloudwick on 7/1/2018.
  */
object HBase_Read_Oracle_Insert {
  //implicit val formats = DefaultFormats
  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.addResource("file:/etc/hbase/conf/hbase-site.xml")
    //conf.set("hbase.zookeeper.quorum", "quickstart.cloudera")
    //conf.set("hbase.zookeeper.property.clientPort", "2181")

    val conn = ConnectionFactory.createConnection(conf)

    val table: Table = conn.getTable(TableName.valueOf("job_status"))
    val scan: Scan = new Scan()
    val scanner: ResultScanner = table.getScanner(scan)

    val iter = scanner.iterator()
    while (iter.hasNext) {
      val record = iter.next()
      val rowKey = Bytes.toString(record.getRow())
      println("Row Key :: " + rowKey)
      val failMsg = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("fail_msg")))
      println("f:fail_msg :: " + failMsg)
      val st = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("st")))
      println("f:st :: " + st)
      val cfg = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("cfg")))
      println("f:cfg :: " + cfg)
      val addTimeStamp = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("add_ts")))
      println("f:add_ts :: " + addTimeStamp)
      val doneTimeStamp = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("done_ts")))
      println("f:done_ts :: " + doneTimeStamp)

      val stepStatusUrl = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("getStepStatusUrl")))
      /*
      val json = parse(stepStatusUrl)
      val allStepDetails = (json \\ "stepDetails").children
      var flag = true
      var count = 0
      for(steps <- allStepDetails) {
        val stepDetails = steps \\ classOf[JArray]
        stepDetails.foreach(x => {
          if(!x(0).toString.equalsIgnoreCase("completed")) {
            count = count + 1
          }
        })
      }

      val finalStatus = if(count == 0) "completed" else "failed"
      println("Final Status :: " + finalStatus)
      */
    }
  }
}