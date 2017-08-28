import scala.collection.mutable.HashMap
import io.swagger.client.model._
import io.swagger.client.api.LocatedCellList
import java.util.NoSuchElementException
import akka.actor._
import Blacs._

object Formula {
  def props(lcell: LocatedCell, master: ActorRef) = Props(new Formula(lcell, master))
  final case object Finished
  final case class Ok(c: Coordinates)
  final case class Not_well_defined(c: Coordinates)
  final case class Deads(l: List[LocatedCell], new_time :Int)
  final case class Start(l: List[(Coordinates, ActorRef)], time: Int)
  final case class Parents(parents: List[(Coordinates, ActorRef)])
  final case object Not_well_defined_exception extends Exception
}

class Formula(lcell: LocatedCell, master: ActorRef) extends Actor {

  import Formula._

  type t = (Coordinates, ActorRef)

  var value = -1

  val deps_table = new HashMap[Coordinates, (ActorRef, Option[Int])]()

  val parents_table = new HashMap[Coordinates, (ActorRef,Boolean)]()

  var master_notified = false

  var time = 0

  def ignore(x: Any) = ()

  def assign_cell(coords: Coordinates, cell: Cell) =
    write(time, coords, 1, 1, cell :: Nil)

  def error() = {
    val cell = Cell(lcell.cell.definition,null_int_val)
    assign_cell(lcell.coords, cell)
  }

  def definition_data_of_cell(c :Cell) = {
    val data = c.definition.data
    (data(0), data(1), data(2), data(3), data(4))
  }

  def tell_parents(sons: List[t]) =
    sons.foreach(x =>
      x._2 ! Parents((lcell.coords, self) :: Nil))

  def propagate_error() =
    parents_table.foreach(x =>
      if (!x._2._2)
        x._2._1 ! Not_well_defined(lcell.coords))

  def check_no_new_cycle(parents: List[t]) =
    !(parents exists(x =>
      deps_table.contains(x._1)))

  def add_new_dependent(x:t) = {
    val (c,a) = x 
    try
      ignore(parents_table(c))
    catch {
      case e: NoSuchElementException =>
        parents_table += ((c,(a,false)))
    }
  }

  def propagate_dependents_or_finish() = {
    def parents_to_list () =
      parents_table.foldLeft(Nil:List[t])((acc, x) =>
        (x._1, x._2._1) :: acc)
    lazy val parents_list =
      (lcell.coords, self) :: (parents_to_list)
    if (deps_table.isEmpty)
      parents_table.foreach(x =>
        if (!x._2._2)
          x._2._1 ! Ok(lcell.coords))
    else
      deps_table.foreach(x =>
        x._2._1 ! Parents(parents_list))
  }

  def check_cycle_and_propagate_dependents(parents: List[t]) = {
    if (check_no_new_cycle(parents) && (value != -1))
      propagate_dependents_or_finish
    else
      not_well_defined
  }

  def assign_and_finish(value: Int) = {
    val c = cell(Some(value), lcell.cell.definition)
    assign_cell(lcell.coords, c)
    parents_table.foreach(x =>
      if (!x._2._2)
        x._2._1 ! Ok(lcell.coords))
    if (!master_notified) {
      master ! Finished
      master_notified = true
    }
  }

  def set_dependency_value(c: Coordinates, v: Int) = {
    val actor = deps_table(c)._1
    try {

      deps_table += ((c, (actor, Some(v))))
    }
    catch {
      case e: java.lang.NullPointerException =>
        deps_table += ((c, (actor, None)))
    }
  }

  def update_and_maybe_finish(c: Coordinates) = {
    val x = read(time, c, 1, 1).l.head
    val vdata = x.cell.value.data
    set_dependency_value(c, vdata)
    if (vdata == lcell.cell.definition.data(4))
      value += 1
    if (deps_table.forall(x => (x._2._2 != None)))
      assign_and_finish(value)
  }

  def count(i: Int, cells: List[LocatedCell]) =
    cells.foldLeft(0)(
      (acc, c) =>
      c match {
      case LocatedCell(
        Cell(
          Definition("int", _, _),
          Value("int", v)),
        _) if (v == i) =>
        acc + 1
      case _ => acc
    })

  def partial_eval_formula_definition(d: Definition) = {
    val i = d.data(4)
    val cells =
      read(
        time,
        Coordinates(d.data(0), d.data(1)),
        d.data(2),
        d.data(3)).l
    count(i, cells)
  }


  def not_well_defined() = {
      error
      propagate_error
      value = -1
      if (!master_notified) {
        master ! Finished
          master_notified = true
      }
  }

  def start(l: List[t]) = {
    lazy val partial_value =
      partial_eval_formula_definition(lcell.cell.definition)
    l match {
      case Nil => {
        if (value == -1)
          value = partial_value
        assign_and_finish(value)
      }
      case l if (l.contains((lcell.coords, self))) =>
        not_well_defined
      case l => {
        if (value == -1)
          value = partial_value
        l.foreach(x =>
          deps_table += ((x._1, (x._2, None))))
        tell_parents(l)
      }
    }
  }

  def constants_diff(new_time: Int) = {
    val (c,r,l,w,i) = definition_data_of_cell(lcell.cell)
    val coords = Coordinates(c,r)
    val old_values = Blacs.read(time, coords, l, w).l
    val current_values = Blacs.read(new_time,coords, l, w).l
    val current_map = new HashMap[Coordinates, Cell]()
    current_values.foreach(x => current_map += ((x.coords, x.cell)))
    old_values.foreach(x => {
      val c = current_map(x.coords)
      if (x.cell != c)
        if (c.value.data == i)
          value +=1
        else
          value -=1})
  }

  def remove_deads(deads: List[LocatedCell], new_time : Int) = {
    deads.foreach(x =>
      try {
        parents_table -= x.coords
        if (deps_table.contains(x.coords)) {
          deps_table -= x.coords
          val c = read(new_time,x.coords,1,1).l.head
          if (c.cell.value == lcell.cell.definition.data(4))
            value -= 1
        }
      }catch {
        case e:NoSuchElementException => ()
      })
  }

  def refresh_depencies()= {
    def f(x:(Coordinates,(ActorRef,Option[Int]))) =
      deps_table += ((x._1,(x._2._1,None)))
    val i = lcell.cell.definition.data(4)
    deps_table.foreach(x =>
      x._2 match {
        case (_,Some(j)) if (i == j) => {
          value -= 1
          f(x)
        }
        case _ => f(x)})
  }

  def receive = {
    case Not_well_defined(c) =>{
      deps_table -= c
      parents_table -= c
      not_well_defined()
    }
    case Parents(parents) => {
      parents.foreach(add_new_dependent)
      check_cycle_and_propagate_dependents(parents)
    }
    case Ok(c: Coordinates) => {
      update_and_maybe_finish(c)
    }
    case Start(l, time) => {
      this.time = time
      start(l)
    }
    case Deads(lc,new_time) => {
      remove_deads(lc, new_time)
      if (value != -1) 
        constants_diff(new_time)
      refresh_depencies()
      master_notified = false      
    }
  }

}
