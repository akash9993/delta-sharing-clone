package io.delta.sharing.server.utils

import java.sql.{Connection, DriverManager, PreparedStatement}
import org.slf4j.LoggerFactory

object DatabaseHelper {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Azure SQL Server Connection String
   private val url = "jdbc:sqlserver://ceer-ods.database.windows.net:1433;database=data-marketplace-poc;user=dbadmin;password=Ceer@123456;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

  // Function to insert request info into the database
  def logRequest(requestType: String, requestData: String): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null

    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Insert Query
      val query = "INSERT INTO RequestLogs (RequestType, RequestData, Timestamp) VALUES (?, ?, GETDATE())"

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, requestType)
      preparedStatement.setString(2, requestData)
      preparedStatement.executeUpdate()

      logger.info(s"✅ Successfully logged request: $requestType")
    } catch {
      case e: Exception =>
        logger.error("❌ Error logging request to database", e)
    } finally {
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }
}
