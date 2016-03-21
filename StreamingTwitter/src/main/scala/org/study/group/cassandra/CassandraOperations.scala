package org.study.group.cassandra

/**
  * Created by egarcia on 22/02/16.
  */

import com.datastax.driver.core.{Cluster, Session}

object CassandraOperations {
  def connectCluster (contactPoint: String): Cluster ={
  Cluster.builder.addContactPoint(contactPoint).build
  }

  def connectSession(cluster: Cluster): Session ={
    cluster.connect
  }

  def disconnect (cluster:Cluster,session:Session): Unit={
    cluster.close
    session.close
  }

  def createKeySpace(keySpaceName: String,cluster:Cluster,session:Session):Unit = {
    val createKeyspaceQuery: String = "CREATE KEYSPACE IF NOT EXISTS "+keySpaceName+" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"
    session.execute(createKeyspaceQuery)
    System.out.println("KeySpace created successfully")
  }

  def createTable(keySpaceName: String, tableName: String, cluster:Cluster,session:Session,atributes:Map[String,String]):Unit = {
    session.execute("USE " + keySpaceName)
    cluster.connect(keySpaceName)

    var subquery = ""
    atributes.foreach { i =>
      subquery += i._1 + " " + i._2.toString +","
    }

    val query = ("CREATE TABLE IF NOT EXISTS " + tableName+ "(id text PRIMARY KEY, " + subquery +");").replace(",)", ")")
    session.execute(query)
    System.out.println("Table created successfully")
  }

  def createTable2(keySpaceName: String, tableName: String, cluster:Cluster,session:Session,atributes:Map[String,String]):Unit = {
    session.execute("USE " + keySpaceName)
    cluster.connect(keySpaceName)

    var subquery = "(id text PRIMARY KEY, "
    atributes.keys.foreach { i =>
      subquery += i.toString + " " + atributes(i).toString + ","
    }

    val query: String = ("CREATE TABLE IF NOT EXISTS " + tableName + subquery + ");").replace(",)", ")")
    session.execute(query)
    System.out.println("Table created successfully")
  }

   def insertData(keySpaceName: String, tableName: String, cluster: Cluster, session: Session, atributes:Map[String,String]): Unit = {
     session.execute("USE " + keySpaceName)
     cluster.connect(keySpaceName)

     var subquery = ""
     atributes.values.foreach { value =>
       subquery += "'"+value + "',"
     }
     val query = ("INSERT INTO tableTweets " + atributes.keys.toString + " VALUES ("+ subquery +");").replace("MapLike"," ").replace("Set"," ").replace(",)", ")")
     session.execute(query)
     System.out.println("Data created successfully")
    }
}

