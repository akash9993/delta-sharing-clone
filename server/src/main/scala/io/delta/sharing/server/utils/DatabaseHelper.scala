// scalastyle:off

/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.delta.sharing.server.utils

 import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
 import io.delta.sharing.server.AccessDeniedException

 import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
 import org.slf4j.LoggerFactory

 import java.time.{LocalDate, LocalDateTime}
 import java.time.format.DateTimeFormatter

object DatabaseHelper {
   private val logger = LoggerFactory.getLogger(this.getClass)

   // Azure SQL Server Connection String
   private val url =
     "jdbc:sqlserver://ceer-ods.database.windows.net:1433;" +
       "database=data-marketplace-poc;user=dbadmin;password=Ceer@123456;" +
       "encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;" +
       "loginTimeout=30;"

  def checkTokenPresentInDb(token: String): Boolean = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Insert Query
      val query = "select user_id from user_subscriptions where token = ?"

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, token)
      resultSet= preparedStatement.executeQuery();

      return resultSet.next()
    } catch {
      case e: Exception =>
        logger.error("Error checking token in database", e)
        false // Return false if there's an error
    } finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }

  def validateUserSubscriptionAndQueryLimit(userId: String, productCatalogId: String): Boolean = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Query
      val query = "SELECT expiration_date, subscription_pricing_detail, queries_used FROM user_subscriptions WHERE user_id = ? AND product_catalog_id = ?"

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, userId)
      preparedStatement.setString(2, productCatalogId)
      resultSet = preparedStatement.executeQuery()

      if (resultSet.next()) {
        // Extract values from result set
        val expirationDateTimeStr = resultSet.getString("expiration_date")
        val subscriptionPricingDetail = resultSet.getString("subscription_pricing_detail")
        val queriesUsed = resultSet.getInt("queries_used")
        logger.info("detail: {}", subscriptionPricingDetail);
        // Parse expiration_date as LocalDateTime
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS") // Adjust if necessary
        val expirationDateTime = LocalDateTime.parse(expirationDateTimeStr, formatter)
        logger.info("expirationDateTime: {}", expirationDateTime);
        val currentDateTime = LocalDateTime.now()

        // Check if subscription has expired
        if (expirationDateTime.isBefore(currentDateTime)) {
          throw new AccessDeniedException("Your subscription plan has expired")
        }

        // Parse JSON to extract queryLimit
        val objectMapper = new ObjectMapper();
        val jsonNode: JsonNode = objectMapper.readTree(subscriptionPricingDetail)
        val queryLimit = jsonNode.get("queryLimit").asInt()

        // Check if query limit is reached
        if (queriesUsed >= queryLimit) {
          throw new AccessDeniedException("Your query limit has been reached")
        }

        true // Valid subscription and within query limit
      } else {
        throw new AccessDeniedException("Data not found")
      }
    } catch {
      case e: Exception =>
        logger.error("Error validating user subscription", e)
        throw new Exception(e.getMessage)
    } finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }
  def updateUserQueryAuditTable(userId: String, productCatalogId: String, productCatalogName: String): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    var resultSet: ResultSet = null
    try {
      // Establish connection
      connection = DriverManager.getConnection(url)

      // Define SQL Insert Query
      val query = "SELECT queries_used FROM user_subscriptions WHERE user_id = ? AND product_catalog_id = ?"

      // Prepare and execute statement
      preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, userId)
      preparedStatement.setString(2, productCatalogId)
      resultSet = preparedStatement.executeQuery()

      if (resultSet.next()) {
        // Extract values from result set
        val queriesUsed = resultSet.getInt("queries_used")
        val query1 = "INSERT INTO user_query_audit (user_id, catalog_id, catalog_name, query_count, time_created) VALUES (?, ?, ?, ?, ?)"
        preparedStatement = connection.prepareStatement(query1)
        preparedStatement.setString(1, userId)
        preparedStatement.setString(2, productCatalogId)
        preparedStatement.setString(3, productCatalogName)
        preparedStatement.setInt(4, queriesUsed+1)
        preparedStatement.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()))
        preparedStatement.executeUpdate();
      }
    } catch {
      case e: Exception =>
        logger.error("Error checking token in database", e)
        throw new Exception(e.getMessage)
    } finally {
      // Close resources in reverse order
      if (resultSet != null) resultSet.close()
      if (preparedStatement != null) preparedStatement.close()
      if (connection != null) connection.close()
    }
  }
}
