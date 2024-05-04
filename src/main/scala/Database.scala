case class Database(tables: List[Table]) {
  override def toString: String = tables.mkString("\n")

  def create(tableName: String): Database = {
    if (!tables.exists(_.name == tableName)) {
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
    val selectedTables = tables.filter(t => tableNames.contains(t.name))
    if (selectedTables.length == tableNames.length) {
      Some(Database(selectedTables))
    } else {
      None
    }
  }

  private def findTable(tableName: String): Option[Table] = {
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
                  key -> value
                } else {
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

        val remainingRows1 = t1.rows.filterNot(row1 => matchedRows.exists(row => row(c1) == row1(c1)))
        // Extragem numele coloanelor din matchedRows și remainingRows1
        val matchedColumns = matchedRows.flatMap(_.keys).toSet
        val remainingColumns = remainingRows1.flatMap(_.keys).toSet
        // Găsim diferența dintre numele coloanelor
        val missingColumns = matchedColumns.diff(remainingColumns)
        val rowsWithMissingColumns = missingColumns.foldLeft(remainingRows1) { (rows, column) =>
          val newRow = rows.map(row => row + (column -> "")) // Adăugăm coloana lipsă cu virgulă
          newRow
        }
        val remainingRows2 = t2.rows.filterNot(row2 => matchedRows.exists(row => row(c1) == row2(c2)))
        val remainingRows2Renamed = remainingRows2.map { row =>
          val updatedRow = row.map { case (key, value) =>
            if (key == c2) c1 -> value // Redenumește cheia corespunzătoare lui c2 în c1
            else key -> value
          }
          updatedRow
        }

        val matchedColumns2 = matchedRows.flatMap(_.keys).toSet
        val remainingColumns2 = remainingRows2Renamed.flatMap(_.keys).toSet


        val missingColumns2 = matchedColumns2.diff(remainingColumns2)
        val rowsWithMissingColumns2 = missingColumns2.foldLeft(remainingRows2Renamed) { (rows, column) =>
          val newRow = rows.map(row => row + (column -> "")) // Adăugăm coloana lipsă cu virgulă
          newRow
        }


        val finalll = matchedRows ++ rowsWithMissingColumns ++ rowsWithMissingColumns2


        Table("joined", finalll)

      }
    }
  }

  //  def join(table1: String, c1: String, table2: String, c2: String): Option[Table] = {
  //    findTable(table1).flatMap { t1 => // Căutăm tabela 1
  //      findTable(table2).map { t2 => // Căutăm tabela 2
  //        val matchedRows = t1.rows.flatMap { row1 => // Pentru fiecare rând din tabela 1
  //          t2.rows.filter(row2 => row1(c1) == row2(c2)).map { row2 => // Selectăm rândurile din tabela 2 care se potrivesc
  //            val newRow = row1 ++ row2 // Unim rândurile
  //            val combinedAddress = combineAddresses(row1("address"), row2("address")) // Combinăm adresele
  //            newRow.updated("address", combinedAddress) // Actualizăm rândul pentru a include adresa combinată
  //          }
  //        }
  //
  //        val remainingRows1 = t1.rows.filterNot(row1 => matchedRows.exists(row => row(c1) == row1(c1)))
  //        val remainingRows1WithExtraColumns = remainingRows1.map { row =>
  //          // Adaugăm noile coloane și le setăm valoarea implicită la virgulă
  //          row + ("salary" -> "", "title" -> "")
  //        }
  //
  //        // Copiem rândurile din tabela 2 care nu au fost îmbinate, mutând "person_name" în "name"
  //        val remainingRows2 = t2.rows.filterNot(row2 => matchedRows.exists(row => row(c2) == row2(c2)))
  //        val remainingRows2WithUpdatedName = remainingRows2.map { row =>
  //          row + ("name" -> row("person_name"))
  //        }
  //
  //        val remainingRow2WithExtraColumns = remainingRows2WithUpdatedName.map { row =>
  //          // Adaugăm noile coloane și le setăm valoarea implicită la virgulă
  //          row + ("age" -> "")
  //        }
  //        val joinedRows = matchedRows ++ remainingRows1WithExtraColumns ++ remainingRow2WithExtraColumns.map(row => row + ("address" -> row("address")))
  //        val finalTable = joinedRows.map(row => row - "person_name")
  //        Table("joined", finalTable)
  //      }
  //    }
  //  }

  // Implement indexing here
  def apply(index: Int): Table = {
    tables(index)
  }
}
