/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiji.spark.connector

import org.apache.hadoop.hbase.security.token.TokenUtil
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiURI
import org.kiji.spark.connector.rdd.KijiRDD
import org.slf4j.LoggerFactory

/** Provides Kiji-specific methods on `SparkContext` */
class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable {

  import SparkContextFunctions._

  /** Returns a view of a Kiji table as `KijiRDD[T]`.
    * This method is made available on `SparkContext` by importing `org.kiji.spark._`
    *
    * @param uri A KijiURI.
    * @param dataRequest A KijiDataRequest.
    * @param vClass ??? Need to talk to Adam.
    * @return An instance of a KijiRDD.
    */
  def kijiRDD[T](uri: KijiURI, dataRequest: KijiDataRequest, vClass: Class[_ <: T]): KijiRDD[T] = {
    val authMode = sc.hadoopConfiguration.get("hbase.security.authentication")
    Log.info(s"Running with $authMode authentication.")

    UserGroupInformation.setConfiguration(sc.hadoopConfiguration)

    val sparkConf = sc.getConf

    val kerberosUsername = sparkConf.getOption("spark.kiji.kerberos.username")
    val keytab = sparkConf.getOption("spark.kiji.kerberos.keytab")

    // If the user specified both properties, then attempt to authenticate
    val ugi = if (kerberosUsername.nonEmpty && keytab.nonEmpty) {
      val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        kerberosUsername.get,
        keytab.get
      )
      // Even if we authenticated, only request a token if security is enabled.
      if (UserGroupInformation.isSecurityEnabled) {
        TokenUtil.obtainAndCacheToken(sc.hadoopConfiguration, ugi)
        Log.info("Obtained and cached auth token for HBase.")
      }
      ugi
    } else {
      // Otherwise assume we are either on a non-secure cluster or the HBase auth token
      // has already been cached by the user.
      UserGroupInformation.getCurrentUser
    }

    val credentials = ugi.getCredentials
    KijiRDD(sc, sc.hadoopConfiguration, credentials, uri, dataRequest).asInstanceOf[KijiRDD[T]]
  }
}

object SparkContextFunctions {
  private final val Log = LoggerFactory.getLogger("SparkContextFunctions")
}