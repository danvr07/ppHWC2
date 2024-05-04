type Row = Map[String, String]
type Tabular = List[Row]

case class Table(tableName: String, tableData: Tabular) {

  override def toString: String = {
    val header = tableData.head.keys.mkString(",")
    val data = tableData.map(_.values.mkString(",")).mkString("\n")
    s"$header\n$data"
  }

  def insert(row: Row): Table = {
    if (tableData.contains(row)) {
      this
    } else {
      Table(tableName, tableData :+ row)
    }
  }

  def delete(row: Row): Table = {
    Table(tableName, tableData.filterNot(_ == row))
  }

  def sort(column: String): Table = {
    Table(tableName, tableData.sortBy(_.getOrElse(column, "")))
  }

  def deleteColumn(column: String): Table = {
    Table(tableName, tableData.map(row => row - column))
  }

  def update(f: FilterCond, updates: Map[String, String]): Table = {
    val updatedRows = tableData.map { row =>
      if (f.eval(row).contains(true)) {
        row ++ updates
      } else {
        row
      }
    }
    Table(tableName, updatedRows)
  }

  def filter(f: FilterCond): Table = {
    val filteredRows = tableData.filter { row =>
      f.eval(row).contains(true)
    }
    Table(tableName, filteredRows)
  }


  def select(columns: List[String]): Table = {
    val newData = tableData.map { row =>
      columns.map(col => col -> row(col)).toMap
    }
    Table(tableName, newData)
  }

  def header: List[String] = tableData.head.keys.toList

  def data: Tabular = tableData

  def name: String = tableName
}

object Table {
  def apply(name: String, s: String): Table = {
    val lines = s.split("\n").toList
    val separator = lines.head match {
      case line if line.contains(",") => ","
      case line if line.contains("\t") => "\t"
      case _ => ","
    }
    val header = lines.head.split(separator).toList
    val data = lines.tail.map(_.split(separator).toList).map(row => header.zip(row).toMap)
    Table(name, data)
  }


  extension (table: Table) {
    def columns: List[String] = table.tableData.headOption.map(_.keys.toList).getOrElse(List.empty)
    def rows: List[Row] = table.tableData
    def todo(index: Int): Option[Row] = {
      if (index >= 0 && index < table.tableData.length) {
        Some(table.tableData(index))
      } else {
        None
      }
    }


  }
}
