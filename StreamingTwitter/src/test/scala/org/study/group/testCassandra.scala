package org.study.group

/**
  * Created by egarcia on 15/03/16.
  */

import com.datastax.driver.core.{Cluster, Session}
import org.study.group.cassandra.CassandraOperations
import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers

class testCassandra extends FlatSpec with ShouldMatchers {
      "testCassandra" should "pass" in {
          val cluster: Cluster = CassandraOperations.connectCluster("127.0.0.1")
          val session: Session = CassandraOperations.connectSession(cluster)
          val nameKeyspace = "keyspaceTweets"
          val nameTable = "tableTweets"

          CassandraOperations.createKeySpace(nameKeyspace, cluster, session)
          CassandraOperations.createTable(nameKeyspace, nameTable, cluster, session, Map("user" -> "text",
                                                                                         "tweet" -> "text"))

          CassandraOperations.insertData(nameKeyspace, nameTable, cluster, session, Map("id" -> "0",
          "user" -> "Pepe",
          "tweet" -> "The best"))

          CassandraOperations.disconnect(cluster, session)

      }

}
