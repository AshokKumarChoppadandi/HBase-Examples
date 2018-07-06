/*
import java.sql.DriverManager

/**
  * Created by cloudwick on 7/1/2018.
  */
object Oracle_Insert {
  def main(args: Array[String]): Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val con = DriverManager.getConnection("jdbc:oracle:thin:@cdtsorl363p.rxcorp.com:1521:PONE2","username","password")
    val sqlStatement =
      """
        | INSERT INTO job_status (jobId, fail_msg, status, cfg, start_ts, end_ts, all_step_status, parent_col)
        | VALUES ('1234', 'Failed Message', 'Completed', 'CFG Message', '132133432', '124214123', 'completed', 'null')
      """.stripMargin
    val st = con.createStatement()
    st.execute(sqlStatement)
    println("Inserted...!!!!")
  }
}
*/