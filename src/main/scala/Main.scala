import scala.collection.mutable.HashMap
import io.swagger.client.model.LocatedCell
import io.swagger.client.model.Coordinates
import io.swagger.client.model.Cell
import io.swagger.client.model.Definition
import io.swagger.client.model.Value
import io.swagger.client.api.LocatedCellList
import java.util.NoSuchElementException

sealed abstract class Color
final case class Black() extends Color
final case class White() extends Color

final case class Not_well_defined() extends Exception

object Main {
  var marks = new HashMap[Coordinates, Color]()

  def assign_cell(coords: Coordinates, cell: Cell) = {
    Blacs.write(0, coords, 1, 1, List(cell))
    mark_cell_as_evaluated(coords)
  }

  def error(coords: Coordinates, f: Definition) = {
    val cell = Cell(f, Blacs.null_int_val)
    assign_cell(coords, cell)
  }

  def mark_cell_as_being_evaluated(c: Coordinates) =
    marks += ((c, Black()))

  def mark_cell_as_evaluated(c: Coordinates) =
    marks += ((c, White()))

  def is_being_evaluated(c: Coordinates) =
    try marks(c) match {
      case Black() => true
      case _ => false
    } catch {
      case e: NoSuchElementException => false
    }

  def is_evaluated(c: Coordinates) =
    try marks(c) match {
      case White() => true
      case _ => false
    } catch {
      case e: NoSuchElementException => false
    }

  def count(i: Int, cells: List[LocatedCell]) = {
    var x = 0
    def aux(c: LocatedCell) =
      c match {
        case LocatedCell(Cell(_, Value("int", v)), _) =>
          if (i == v) { x += 1 }
        case _ => ()
      }
    cells.foreach(aux)
    x
  }

  def eval_formula_definition(f: Definition) =
    f match {
      case Definition("count", List(c, r, l, w, v), _) => {
        val cells = (Blacs.read(0, Coordinates(c, r), l, w))
        val i = count(v, cells.l)
        Blacs.cell(Some(i), f)
       }
      case _ => {
        Blacs.cell(None, f)
      }
    }

  def dependencies(formulas: List[LocatedCell], d: Definition) =
    d match {
      case Definition("count", List(c, r, l, w, _), _) => {
        val inf = Coordinates(c, r)
        val sup = Coordinates(c + l, r + w)
        def range(c: LocatedCell) = {
          (inf <= c.coords) && (c.coords <= sup)
        }
        formulas.filter(range)
      }
      case _ => List()
    }

  def eval_formula(formulas: List[LocatedCell], lcell: LocatedCell): Unit = {
    val definition = lcell.cell.definition
    val coords = lcell.coords
    if (is_evaluated(coords))
      ()
    else if (is_being_evaluated(coords)) {
      error(coords, definition)
      throw Not_well_defined()
    } else {
      mark_cell_as_being_evaluated(coords)
      try {
        val deps = dependencies(formulas, definition)
        def f(c: LocatedCell) = eval_formula(formulas, c)
        deps.foreach(f)
        assign_cell(coords, (eval_formula_definition(definition)))
      } catch {
        case e: Not_well_defined => ()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    Blacs.init
    val time = Blacs.get_time
    val size = Blacs.get_size
    val formulas = (Blacs.read_formulas(time, size._1, size._2)).l
    def f(c:LocatedCell) = eval_formula(formulas, c)
    formulas.foreach(f)
  }
}
