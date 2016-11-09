import io.gatling.app._
import io.gatling.core.scenario.Simulation

object Main {

  type SimulationFactory = (Class[_ <: Simulation]) => Simulation

  def main(args: Array[String]): Unit = {
    Gatling.fromArgs(args, Some(classOf[Simulation]), _ => new ElasticSimulation)
  }
}
