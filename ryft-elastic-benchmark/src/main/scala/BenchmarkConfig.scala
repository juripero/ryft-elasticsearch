import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._
import scala.util.Try

case class ElasticScenario(name: String, queries: List[ElasticQuery])

case class ElasticQuery(body: String, name: String, url: String, repeat: Int)

object BenchmarkConfig {
  private val configuration = ConfigFactory.load()

  val baseUrl = configuration.getString("url")
  val threads = configuration.getInt("threads")

  private def getQueries: List[ElasticQuery] = {
    configuration.getObjectList("scenario.queries").asScala.map { configObject =>
      val map = configObject.unwrapped()
      val name = map.get("name").toString
      val body = map.get("body").toString
      val url = map.get("url").toString
      val repeat = Try(map.get("repeat").toString.toInt).toOption.getOrElse(1)
      ElasticQuery(body, name, url, repeat)
    }.toList
  }

  val scenario = ElasticScenario(configuration.getString("scenario.name"), getQueries)
}
