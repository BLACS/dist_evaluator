import io.swagger.client.api.ClockApi
import io.swagger.client.api.IoApi
import io.swagger.client.api.LocatedCellList
import io.swagger.client.model.ReadRequest
import io.swagger.client.model.ReadPromise
import io.swagger.client.model.Coordinates
import io.swagger.client.model.WriteRequest
import io.swagger.client.model.Dimensions
import io.swagger.client.model.Cell
import io.swagger.client.model.Value
import io.swagger.client.model.Definition
import io.swagger.client.model.LocatedCell
import java.time.Instant

object Blacs {

  val host = "http://172.17.0.2:8080"
  val io_api = new IoApi(host)
  val clock_api = new ClockApi(host)
  val sheet = "noname"
  val tag = "alice"
  val default = "bob"

  def gettimeofday() =
    Instant.now.toEpochMilli.toDouble / 1000.0

  def get_size() =
    io_api.sizeSheet(sheet) match {
      case Some(Dimensions(l, w)) => (l, w)
      case None => throw new Exception
    }

  def get_time() =
    clock_api.timeSheet(sheet) match {
      case Some(t: Int) => t
      case None => throw new Exception
    }

  def get_hash(hash: String) =
    io_api.hashSheet(sheet, hash) match {
      case Some(l) => l
      case None => throw new Exception
    }

  def read_and_hash(rrq: ReadRequest): LocatedCellList =
    io_api.readSheet(sheet, rrq) match {
      case Some(ReadPromise(date, hash)) =>
        if (date > gettimeofday()) {
          val wait_time = date - (gettimeofday()) + 0.005
          Thread.sleep(wait_time.toLong)
          get_hash(hash)
        } else
          get_hash(hash)
      case None => throw new Exception
    }

  def read_sheet(time: Int, length: Int, width: Int) =
    read_and_hash(ReadRequest(tag, time, (Coordinates(0, 0)), length, width, false, default))

  def read_formulas(time: Int, length: Int, width: Int) =
    read_and_hash(ReadRequest(tag, time, (Coordinates(0, 0)), length, width, true, default))

  def read(time: Int, origin: Coordinates, length: Int, width: Int) =
    read_and_hash(ReadRequest(tag, time, origin, length, width, false, default))

  def write_sheet(time: Int, length: Int, width: Int, cells: List[Cell]) =
    io_api.writeSheet(sheet, WriteRequest(tag, time, (Coordinates(0, 0)), length, width, cells))

  def write(time: Int, origin: Coordinates, length: Int, width: Int, cells: List[Cell]) =
    io_api.writeSheet(sheet, WriteRequest(tag, time, origin, length, width, cells))

  def none_val =
    Value("none", null)

  def null_int_val =
    Value("int", null)

  def int_val(i: Int) =
    Value("int", i)

  def value(i: Option[Int]) =
    i match {
      case Some(d) => int_val(d)
      case None => none_val
    }

  def none_def =
    Definition("none", null, null)

  def null_int_def =
    Definition("int", null, null)

  def int_def(i: Int) =
    Definition("int", List(i), null)

  def count_def(c: Int, r: Int, l: Int, w: Int, v: Int) =
    Definition("count", List(c, r, l, w, v), null)

  def cell(v: Option[Int] = None, definition: Definition) = {
    val x = value(v)
    Cell(definition, x)
  }

  def init() = {
    val l = List(
      cell(Some(1), int_def(1)), cell(Some(2), int_def(2)), cell(Some(3), int_def(3)), cell(None, count_def(0, 0, 4, 1, 1)),
      cell(Some(1), int_def(1)), cell(Some(2), int_def(2)), cell(Some(3), int_def(3)), cell(None, count_def(0, 1, 4, 1, 2)),
      cell(Some(1), int_def(1)), cell(Some(2), int_def(2)), cell(Some(3), int_def(3)), cell(None, count_def(0, 2, 4, 1, 3)),
      cell(Some(1), int_def(1)), cell(Some(2), int_def(2)), cell(Some(3), int_def(3)), cell(None, count_def(0, 3, 4, 1, 4))
    )
    val wr = WriteRequest(tag, 0, Coordinates(0, 0), 4, 4, l)
    io_api.writeSheet(sheet, wr)
  }
}
