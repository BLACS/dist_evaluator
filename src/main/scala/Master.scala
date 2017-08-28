import scala.collection.mutable.HashMap
import io.swagger.client.model._
import io.swagger.client.api.LocatedCellList
import java.util.NoSuchElementException
import akka.actor._
import Blacs._

object Master {

  final case class Formulas(l: List[LocatedCell], time: Int)

  def props = Props[Master]

}

class Master() extends Actor {

  import Master._

  import Formula._

  var time = 0

  var children = 0

  var actors = new HashMap[Coordinates, ActorRef]()

  def complement = (a: List[LocatedCell], b: List[LocatedCell]) =>
    a.foldLeft(Nil: List[LocatedCell])((l, x) =>
      if (!b.exists(y =>
        x.cell.definition == y.cell.definition))
        (x :: l)
      else
        l)

  def dependencies(formulas: List[LocatedCell], d: Definition) = d match {
      case Definition("count", List(c, r, l, w, _), _) => {
        val inf = Coordinates(c, r)
        val sup = Coordinates(c + l - 1, r + w - 1)
        formulas.filter(c => (inf <= c.coords) && (c.coords <= sup))
      }
      case _ => Nil
    }

  def launch_new_formulas(l: List[LocatedCell]) = l.foreach(x =>
      actors += ((x.coords, context.actorOf(Formula.props(x, self)))))

  def send_dependencies(l: List[LocatedCell], t: Int) = l.foreach(x => {
      val deps = dependencies(l, x.cell.definition)
      val deps_actor = deps.map(x => (x.coords, actors(x.coords)))
      actors(x.coords) ! Start(deps_actor, t)
      children += 1
    })

  def kill_constants(l: List[LocatedCell]) = l.foreach(x =>
    try {
      context.stop(actors(x.coords))
      actors -= x.coords
    } catch {
      case e: NoSuchElementException => ()
    })

  def notify_survivors(l: List[LocatedCell], t: Int) =
    actors.foreach(x => x._2 ! Deads(l, t))

  def update_actors() = {
    val new_time = get_time
    val size = get_size
    val old_formulas =
      read_formulas(time, size._1, size._2).l
    val current_formulas =
      read_formulas(new_time, size._1, size._2).l
    val dead_formulas =
      complement(old_formulas, current_formulas)
    val new_formulas =
      complement(current_formulas, old_formulas)
    kill_constants(dead_formulas)
    notify_survivors(dead_formulas, time)
    launch_new_formulas(new_formulas)
    send_dependencies(current_formulas, new_time)
    time = new_time
  }

  def receive = {
    case Formulas(l, t) => {
      time = t
      launch_new_formulas(l)
      send_dependencies(l, t)
    }
    case Finished => {
      children -= 1
      if (children == 0)
        update_actors()
    }
  }
}
