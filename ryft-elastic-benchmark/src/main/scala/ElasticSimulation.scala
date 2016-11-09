
import io.gatling.core.Predef._
import io.gatling.core.body.StringBody
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._

class ElasticSimulation extends Simulation {

  val httpConf = http.baseURL(BenchmarkConfig.baseUrl)
  setUp(getScenario.inject(atOnceUsers(BenchmarkConfig.threads)).protocols(httpConf))

  def getScenario = {
    var scenarioBuilder = ScenarioBuilder(BenchmarkConfig.scenario.name)
    BenchmarkConfig.scenario.queries.foreach { query =>
      scenarioBuilder = scenarioBuilder.repeat(query.repeat) { exec(http(query.name)
        .post(query.url)
        .body(StringBody(query.body)))
      }
    }
    scenarioBuilder
  }
}
