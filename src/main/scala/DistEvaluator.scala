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
  def props(lcell: LocatedCell, master: ActorRef) = Props(new Formula(lcell,master))
  final case object Not_well_defined
  final case object Finished
  final case class Ok(c: Coordinates)
  final case class Start(l: List[(Coordinates, ActorRef)], time: Int)
  final case class Parents(parents: List[(Coordinates, ActorRef)])
  final case object Not_well_defined_exception extends Exception
}

class Formula(lcell: LocatedCell, master: ActorRef) extends Actor {
  import Formula._

  type t = (Coordinates, ActorRef)

  var value = -1
  var deps_table    = new HashMap[Coordinates, (ActorRef,Boolean)]()
  var parents_table = new HashMap[Coordinates, ActorRef]()
  var master_notified  = false
  var time = 0

  def ignore(x:Any) = ()

  def assign_cell(coords: Coordinates, cell: Cell) =
    Blacs.write(0, coords, 1, 1, cell::Nil)

  def error() = {
    val cell = Cell(lcell.cell.definition, Blacs.null_int_val)
    assign_cell(lcell.coords, cell)
  }

  def tell_parents(sons: List[t]) =
    sons.foreach(x => x._2 ! Parents((lcell.coords, self)::Nil))

  def propagate_error() = parents_table.foreach(x => x._2 ! Not_well_defined)

  def check_no_new_cycle(parents: List[t]) =
    !parents.exists(x => deps_table.contains(x._1))

  def add_new_dependent(rc: t) =
    try
      ignore(parents_table(rc._1))
    catch {
      case e: NoSuchElementException =>
        parents_table += rc
    }

  def propagate_dependents_or_finish() = {
    lazy val parents_list = (lcell.coords, self) :: (parents_table.toList)
    if (deps_table.isEmpty)
      parents_table.foreach(
        x => x._2 ! Ok(lcell.coords))
    else
      deps_table.foreach(
        x => x._2._1 ! Parents(parents_list))
  }

  def check_cycle_and_propagate_dependents(parents: List[t]) = {
    if (check_no_new_cycle(parents) && (value != -1))
      propagate_dependents_or_finish
    else
      self ! Not_well_defined
  }

  def assign_and_finish(value:Int) = {
    val c = Blacs.cell(Some(value), lcell.cell.definition)
    assign_cell(lcell.coords, c)
    parents_table.foreach(x => x._2 ! Ok(lcell.coords))
    if (!master_notified) {
      master ! Finished
      master_notified = true
    }
  }

  def update_and_maybe_finish(c: Coordinates) = {
    val v = deps_table(c)
    deps_table += ((c,(v._1,true)))
    val x = Blacs.read(0, c, 1, 1).l.head
    if (x.cell.value.data == lcell.cell.definition.data(4))
      value += 1
    if (!deps_table.exists(x => !(x._2._2)))
      assign_and_finish(value)
  }

  def count(i: Int, cells: List[LocatedCell]) =
    cells.foldLeft(0)((acc, c) => c match {
      case
          LocatedCell(Cell(Definition("int",_,_),Value("int", v)), _) if (v==i) =>
        acc + 1
      case _ => acc
    })

  def partial_eval_formula_definition(d: Definition) = {
    val i = d.data(4)
    val cells =
      Blacs.read(0,Coordinates(d.data(0),d.data(1)),d.data(2),d.data(3)).l
    count(i,cells)
  }

  def start(l:List[t]) = {
    lazy val partial_value = partial_eval_formula_definition(lcell.cell.definition)
    if (l == Nil) {
      value = partial_value
      assign_and_finish(value)
    } else if (l.contains((lcell.coords,self))) {
      self ! Not_well_defined
    } else {
      value = partial_value
      l.foreach(x => deps_table += ((x._1,(x._2,false))))
      tell_parents(l)
    }
  }

  def receive = {
    case Not_well_defined => {
      error
      propagate_error
      if (!master_notified) {
        master ! Finished
        master_notified = true
      }
    }
    case Parents(parents) => {
      parents.foreach(add_new_dependent)
      check_cycle_and_propagate_dependents(parents)
    }
    case Ok(c: Coordinates) => {
      update_and_maybe_finish(c)
    }
    case Start(l,time) => {
      this.time = time
      start(l)
    }
  }
}

object Master {
  def props = Props[Master]
  final case class Formulas(l: List[LocatedCell])
}

class Master() extends Actor {
  import Master._
  import Formula._

  var children = 0

  var actors   = new HashMap[Coordinates, ActorRef]() 

  def dependencies(formulas: List[LocatedCell], d: Definition) =
    d match {
      case Definition("count", List(c, r, l, w, _), _) => {
        val inf = Coordinates(c, r)
        val sup = Coordinates(c + l - 1, r + w - 1)
        formulas.filter(c => (inf <= c.coords) && (c.coords <= sup))
      }
      case _ => Nil
    }

  def start_actors(l: List[LocatedCell]) = {
    l.foreach(x => actors += ((x.coords,context.actorOf(Formula.props(x,self)))))
    val time = Blacs.get_time()
    l.foreach(x => {
      val deps = dependencies(l, x.cell.definition)
      val deps_actor = deps.map(x => (x.coords, actors(x.coords)))
      actors(x.coords) ! Start(deps_actor,time)
      children +=1
    })
  }

  def receive = {
    case Formulas(l) => start_actors(l)
    case Finished => {
      children -= 1
      if (children == 0){
        context.system.terminate
      }
    }
  }
}

object DistEvaluator {
  import Master._

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("disteval")
    Blacs.init
    val time = Blacs.get_time
    val size = Blacs.get_size
    val formulas = (Blacs.read_formulas(time, size._1, size._2)).l
    try {
      val master = system.actorOf(Master.props, "master")
      master ! Formulas(formulas)
    } catch {
      case e: Exception =>
        system.terminate()
    }
  }
}
