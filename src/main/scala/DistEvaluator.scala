import scala.collection.mutable.HashMap
import io.swagger.client.model.LocatedCell
import io.swagger.client.model.Coordinates
import io.swagger.client.model.Cell
import io.swagger.client.model.Definition
import scala.collection.mutable.HashMap
import io.swagger.client.model.Value
import io.swagger.client.api.LocatedCellList
import java.util.NoSuchElementException
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props

object Formula {
  def props(lcell: LocatedCell, deps: List[(Coordinates, ActorRef)]) =
    Props(new Formula(lcell, deps))
  final case class Ok(c: Coordinates)
  final case class Parents(parents: List[(Coordinates, ActorRef)])
  final case object Not_well_defined
  final case class Not_well_defined_exception() extends Exception
}

class Formula(lcell: LocatedCell, deps: List[(Coordinates, ActorRef)]) extends Actor {
  import Formula._

  var parents_table = new HashMap[Coordinates, ActorRef]()

  def assign_cell(coords: Coordinates, cell: Cell) = {
    Blacs.write(0, coords, 1, 1, List(cell))
  }

  def error() = {
    val cell = Cell(lcell.cell.definition, Blacs.null_int_val)
    assign_cell(lcell.coords, cell)
  }

  def tell_parents(sons: List[(Coordinates, ActorRef)]) = {
    def f(l: (Coordinates, ActorRef)) = (l._2) ! Parents(List((lcell.coords, self)))
    sons.foreach(f)
  }

  def propagate_error() = {
    def f(x: (Coordinates, ActorRef)) =
      x._2 ! Not_well_defined
    parents_table.map(f)
  }

  def check_cycle(deps: List[(Coordinates, ActorRef)], parents: List[(Coordinates, ActorRef)]) = {
    def f(rc: (Coordinates, ActorRef)) = {
      if (deps.contains(rc))
        throw new Not_well_defined_exception()
    }
    try {
      parents.foreach(f)
      true
    } catch {
      case e: Not_well_defined_exception => false
    }
  }

  def add_dependent(rc: (Coordinates, ActorRef)) = {
    try {
      parents_table(rc._1); ()
    } catch {
      case e: NoSuchElementException => parents_table += rc
    }
  }

  def check_cycle_add_and_propagate_dependents(parents: List[(Coordinates, ActorRef)]) = {
    parents.foreach(add_dependent)
    val all_parents = parents_table.toList
    if (check_cycle(deps, parents)) {
      val l = (lcell.coords, self) :: all_parents
      def propagate_dependents(rc: (Coordinates, ActorRef)) =
        rc._2 ! Parents(l)
      deps.foreach(propagate_dependents)
    } else {
      error()
      propagate_error()
    }
  }

  def update_and_maybe_finish(c: Coordinates) = {
    val x = Blacs.read(0, c, 1, 1)
    assert(false)
  }

  def partial_eval_formula_definition(d: Definition, deps: List[(Coordinates, ActorRef)]) = ()

  def receive = {
    case Not_well_defined => {
      error
      propagate_error()
    }
    case Parents(parents) => {
      check_cycle_add_and_propagate_dependents(parents)
    }
    case Ok(c: Coordinates) => {
      update_and_maybe_finish(c)
    }
  }

  tell_parents(deps)
  val partial_value = partial_eval_formula_definition(lcell.cell.definition, deps)
  var value = partial_value

}

object Master {
  def props = Props[Master]
  final case class Formulas(l: List[LocatedCell])
}

class Master extends Actor {
  import Master._

  def startActors(l: List[LocatedCell]) =
    l.foreach({case _ => ()})
    println("Start Actors")

  def receive = {
    case Formulas(l) => startActors(l)
  }
}

object DistEvaluator {
  import Master._

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("disteval")
    // Blacs.init
    // val time = Blacs.get_time
    // val size = Blacs.get_size
    // val formulas = (Blacs.read_formulas(time, size._1, size._2)).l
    println("Hello World!")
    val formulas = List()
    try {
      val master = system.actorOf(Master.props, "master")
      master ! Formulas(formulas)
    } finally {
      system.terminate()
    }
  }
}
