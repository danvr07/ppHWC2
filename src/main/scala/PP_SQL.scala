import scala.language.implicitConversions

trait PP_SQL_DB {
  def eval: Option[Database]
}

case class CreateTable(database: Database, tableName: String) extends PP_SQL_DB {
  def eval: Option[Database] = {
    if (database.tables.exists(_.name == tableName)) {
      // Tabela există deja, nu facem nimic și întoarcem None
      Some(database)
    } else {
      // Tabela nu există, o creăm și o adăugăm la baza de date
      val newTable = Table(tableName, List())
      Some(database.copy(tables = database.tables :+ newTable))
    }
  }
}

case class DropTable(database: Database, tableName: String) extends PP_SQL_DB {
  def eval: Option[Database] = {
    val newTables = database.tables.filterNot(t => t.name == tableName)
    Some(database.copy(tables = newTables))
  }
}

implicit def PP_SQL_DB_Create_Drop(t: (Option[Database], String, String)): Option[PP_SQL_DB] = {
  t match {
    case (Some(db), "CREATE", tableName) => Some(CreateTable(db, tableName))
    case (Some(db), "DROP", tableName) => Some(DropTable(db, tableName))
    case _ => None
  }
}

case class SelectTables(database: Database, tableNames: List[String]) extends PP_SQL_DB {
  def eval: Option[Database] = {
    val newTables = database.tables.filter(t => tableNames.contains(t.name))
    Some(database.copy(tables = newTables))
  }
}

// metoda pentru selectarea unei singure tabele
case class SelectTables_one_table(database: Database, tableName: String) extends PP_SQL_Table {
  def eval: Option[Table] = {
    val table = database.findTable(tableName)
    Some(table.get)
  }
}


implicit def PP_SQL_DB_Select(t: (Option[Database], String, List[String])): Option[PP_SQL_DB] = {
  t match {
    case (Some(db), "SELECT", tableNames) => Some(SelectTables(db, tableNames))
    case _ => None
  }
}

// Query pentru selectarea unei singure tabele
implicit def PP_SQL_Table_Select_one_table(t: (Option[Database], String, String)): Option[PP_SQL_Table] = {
  t match {
    case (Some(db), "SELECTTABLE", tableName) => Some(SelectTables_one_table(db, tableName))
    case _ => None
  }
}

case class JoinTables(database: Database, table1: String, column1: String, table2: String, column2: String) extends PP_SQL_DB {
  def eval: Option[Database] = {

    // Unim tabelele folosind funcția join din obiectul Database
    val joinedTable = database.join(table1, column1, table2, column2)
    (joinedTable) match {
      case Some(joinedTable) => {
        // Șterge tabelele vechi
        val newTables = database.tables.filterNot(t => t.name == table1 || t.name == table2)
        // Adaugă tabela nouă
        Some(database.copy(tables = newTables :+ joinedTable))
      }
      case None => None
    }
  }
}

implicit def PP_SQL_DB_Join(t: (Option[Database], String, String, String, String, String)): Option[PP_SQL_DB] = {
  t match {
    case (Some(db), "JOIN", table1, column1, table2, column2) => Some(JoinTables(db, table1, column1, table2, column2))
    case _ => None
  }
}

trait PP_SQL_Table {
  def eval: Option[Table]
}

case class InsertRow(table: Table, values: Tabular) extends PP_SQL_Table {
  def eval: Option[Table] = {
    val updatedTable = values.foldLeft(table)((accTable, row) => accTable.insert(row))
    Some(updatedTable)
  }
}

implicit def PP_SQL_Table_Insert(t: (Option[Table], String, Tabular)): Option[PP_SQL_Table] = {
  t match {
    case (Some(table), "INSERT", values) => Some(InsertRow(table, values))
    case _ => None
  }
}


case class UpdateRow(table: Table, condition: FilterCond, updates: Map[String, String]) extends PP_SQL_Table {
  def eval: Option[Table] = {
    val updatedTable = table.update(condition, updates)
    Some(updatedTable)
  }
}

implicit def PP_SQL_Table_Update(t: (Option[Table], String, FilterCond, Map[String, String])): Option[PP_SQL_Table] = {
  t match {
    case (Some(table), "UPDATE", condition, updates) => Some(UpdateRow(table, condition, updates))
    case _ => None
  }
}

case class SortTable(table: Table, column: String) extends PP_SQL_Table {
  def eval: Option[Table] = {
    val sortedTable = table.sort(column)
    Some(sortedTable)
  }
}

implicit def PP_SQL_Table_Sort(t: (Option[Table], String, String)): Option[PP_SQL_Table] = {
  t match {
    case (Some(table), "SORT", column) => Some(SortTable(table, column))
    case _ => None
  }
}

case class DeleteRow(table: Table, row: Row) extends PP_SQL_Table {
  def eval: Option[Table] = {
    val updatedTable = table.delete(row)
    Some(updatedTable)
  }
}

implicit def PP_SQL_Table_Delete(t: (Option[Table], String, Row)): Option[PP_SQL_Table] = {
  t match {
    case (Some(table), "DELETE", row) => Some(DeleteRow(table, row))
    case _ => None
  }
}

case class FilterRows(table: Table, condition: FilterCond) extends PP_SQL_Table {
  def eval: Option[Table] = {
    val filteredTable = table.filter(condition)
    Some(filteredTable)
  }
}

implicit def PP_SQL_Table_Filter(t: (Option[Table], String, FilterCond)): Option[PP_SQL_Table] = {
  t match {
    case (Some(table), "FILTER", condition) => Some(FilterRows(table, condition))
    case _ => None
  }
}

case class SelectColumns(table: Table, columns: List[String]) extends PP_SQL_Table {
  def eval: Option[Table] = {
    val selectedTable = table.select(columns)
    Some(selectedTable)
  }
}

implicit def PP_SQL_Table_Select(t: (Option[Table], String, List[String])): Option[PP_SQL_Table] = {
  t match {
    case (Some(table), "EXTRACT", columns) => Some(SelectColumns(table, columns))
    case _ => None
  }
}

def queryT(p: Option[PP_SQL_Table]): Option[Table] = {
  p match {
    case Some(pp) => pp.eval
    case None => None
  }
}

def queryDB(p: Option[PP_SQL_DB]): Option[Database] = {
  p match {
    case Some(pp) => pp.eval
    case None => None
  }
}