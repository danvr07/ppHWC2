object Queries {

  def killJackSparrow(t: Table): Option[Table] = queryT(PP_SQL_Table_Filter(Some(t), "FILTER", row => Some(row("name") != "Jack")))

  def insertLinesThenSort(db: Database): Option[Table] =
    queryDB(PP_SQL_DB_Create_Drop(Some(db), "CREATE", "Inserted Fellas"))
      .flatMap(newDB => queryT(PP_SQL_Table_Insert(queryT(PP_SQL_Table_Select_one_table(Some(newDB), "SELECTTABLE", "Inserted Fellas")), "INSERT", List(
        Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555")
      ))))
      .flatMap(newDB => queryT(PP_SQL_Table_Insert(Some(newDB), "INSERT", List(
        Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142")
      ))))
      .flatMap(newDB => queryT(PP_SQL_Table_Insert(Some(newDB), "INSERT", List(
        Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132")
      ))))
      .flatMap(newDB => queryT(PP_SQL_Table_Insert(Some(newDB), "INSERT", List(
        Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")
      ))))
      .flatMap(newDB => queryT(PP_SQL_Table_Sort(Some(newDB), "SORT", "age")))

  def youngAdultHobbiesJ(db: Database): Option[Table] = queryT(PP_SQL_Table_Select(queryT(PP_SQL_Table_Filter(queryT(PP_SQL_Table_Select_one_table(queryDB(PP_SQL_DB_Join(Some(db), "JOIN", "People", "name", "Hobbies", "name")), "SELECTTABLE", "joined")), "FILTER", row => Some(row("age") < "25" && row("age") >= "1" && row("name").startsWith("J")))), "EXTRACT", List("name", "hobby")))

  //queryT(PP_SQL_Table_Filter(queryT(PP_SQL_Table_Select_one_table(queryDB(PP_SQL_DB_Join(Some(db), "JOIN", "People", "name", "Hobbies", "name")), "SELECTTABLE", "joined")), "FILTER", row => Some(row("age")  < "25" && row("age") > "1" &&  row("name").startsWith("J")))

  //def youngAdultHobbiesJ(db: Database): Option[Table] =
  //  queryT(
  //    PP_SQL_Table_Filter(
  //      queryT(
  //        PP_SQL_Table_Select_one_table(
  //          queryDB(
  //            PP_SQL_DB_Join(Some(db), "JOIN", "People", "name", "Hobbies", "name")
  //          ),
  //          "SELECTTABLE",
  //          "joined"
  //        )
  //      ),
  //      "FILTER",
  //      row => Some(row("age") < "25" && row("age") > "1" && row("name").startsWith("J"))
  //    )
  //  )
  //    queryT(
  //      PP_SQL_Table_Filter(
  //        queryT(
  //          PP_SQL_Table_Select_one_table(
  //            queryDB(PP_SQL_DB_Join(Some(db), "JOIN", "People", "name", "Hobbies", "name")),
  //            "SELECTTABLE",
  //            "joined"
  //          )
  //        ),
  //        "FILTER",
  //        row => Some(row("age").toInt < 25 && row("name").startsWith("J") && row.contains("hobby"))
  //      )
  //    )
  //  def youngAdultHobbiesJ(db: Database): Option[Table] = queryT(PP_SQL_Table_Filter(queryT(PP_SQL_Table_Select_one_table(queryDB(PP_SQL_DB_Join(Some(db), "JOIN", "People", "name", "Hobbies", "name")), "SELECTTABLE", "joined")), "FILTER", row => Some(row("age").toInt < 25)))


}
