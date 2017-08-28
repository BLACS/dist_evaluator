import akka.actor.ActorSystem

object DistEvaluator {
  import Master._

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("disteval")
    Blacs.init()
    val time = Blacs.get_time
    val size = Blacs.get_size
    val formulas = (Blacs.read_formulas(time, size._1, size._2)).l
    try {
      val master = system.actorOf(Master.props, "master")
      master ! Formulas(formulas,time)
    } catch {
      case e: Exception => system.terminate()
    }
  }
}
