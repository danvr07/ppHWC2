case class Database(tables: List[Table]) {
  override def toString: String = tables.mkString("\n")

  def create(tableName: String): Database = {
    if (findTable(tableName).isEmpty) { // Dacă tabela nu există deja) {
      val newTable = Table(tableName, List.empty) // Creăm o nouă tabelă goală
      Database(tables :+ newTable) // Adăugăm noua tabelă la lista de tabele
    } else {
      this // Dacă tabela există deja, returnăm baza de date nemodificată
    }
  }

  def drop(tableName: String): Database = {
    Database(tables.filterNot(_.name == tableName))
  }

  def selectTables(tableNames: List[String]): Option[Database] = {
    val selectedTables = tables.filter(t => tableNames.contains(t.name)) // Filtrăm tabelele selectate
    if (selectedTables.length == tableNames.length) {
      Some(Database(selectedTables))
    } else {
      None
    }
  }


  // metoda pentru a gasi o tabela dupa nume
  def findTable(tableName: String): Option[Table] = {
    tables.find(_.name == tableName)
  }

  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
    findTable(table1).flatMap { t1 =>
      findTable(table2).map { t2 =>
        val matchedRows = t1.rows.flatMap { row1 =>
          t2.rows.filter(row2 => row1(c1) == row2(c2)).map { row2 =>
            val newRow = row1 ++ row2.filterKeys(_ != c2).map { case (key, value) =>
              if (row1.contains(key)) {
                if (row1(key).equals(value)) {
                  // Returnăm valoarea dacă sunt egale
                  key -> value
                } else {
                  // Concatenăm valorile dacă sunt diferite
                  val concatenatedValue = row1(key) + ";" + value
                  key -> concatenatedValue
                }
              } else {
                key -> value
              }
            }
            newRow
          }
        }

        // Extragem rândurile care nu au fost găsite în tabelul 2
        val remainingRows1 = t1.rows.filterNot(row1 => matchedRows.exists(row => row(c1) == row1(c1)))
        // Extragem numele coloanelor din matchedRows și remainingRows1
        val matchedColumns = matchedRows.flatMap(_.keys).toSet
        // Extragem numele coloanelor din remainingRows1
        val remainingColumns1 = remainingRows1.flatMap(_.keys).toSet

        // Găsim diferența dintre numele coloanelor pentru a inlocui coloanele libere cu virgula
        val missingColumns = matchedColumns.diff(remainingColumns1)
        val rowsWithMissingColumns1 = missingColumns.foldLeft(remainingRows1) { (rows, column) =>
          val newRow = rows.map(row => row + (column -> "")) // Adăugăm coloana lipsă cu virgulă
          newRow
        }

        // Extragem rândurile care nu au fost găsite în tabelul 1
        val remainingRows2 = t2.rows.filterNot(row2 => matchedRows.exists(row => row(c1) == row2(c2)))
        val remainingRows2Renamed = remainingRows2.map { row =>
          val updatedRow = row.map { case (key, value) =>
            if (key == c2) c1 -> value // Se redenumeste c2 in c1
            else key -> value
          }
          updatedRow
        }

        // Extragem numele coloanelor din matchedRows și remainingRows2Renamed
        val matchedColumns2 = matchedRows.flatMap(_.keys).toSet

        // Extragem numele coloanelor din remainingRows2Renamed
        val remainingColumns2 = remainingRows2Renamed.flatMap(_.keys).toSet

        // Găsim diferența dintre numele coloanelor pentru a inlocui coloanele libere cu virgula
        val missingColumns2 = matchedColumns2.diff(remainingColumns2)
        val rowsWithMissingColumns2 = missingColumns2.foldLeft(remainingRows2Renamed) { (rows, column) =>
          val newRow = rows.map(row => row + (column -> "")) // Adăugăm coloana lipsă cu virgulă
          newRow
        }

        // Unim toate rândurile
        val joined = matchedRows ++ rowsWithMissingColumns1 ++ rowsWithMissingColumns2

        Table("joined", joined)

      }
    }
  }

  def apply(index: Int): Table = {
    tables(index)
  }
}
