object Queries {

  def killJackSparrow(t: Table): Option[Table] = queryT(PP_SQL_Table_Filter(Some(t), "FILTER", Field("name", _ != "Jack")))

  def insertLinesThenSort(db: Database): Option[Table] =
    queryT(PP_SQL_Table_Sort(queryT(PP_SQL_Table_Insert(queryT(PP_SQL_Table_Select_one_table(queryDB(PP_SQL_DB_Create_Drop(Some(db), "CREATE", "Inserted Fellas")),
      "SELECTTABLE", "Inserted Fellas")), "INSERT", List(Map("name" -> "Ana", "age" -> "93", "CNP" -> "455550555"),
      Map("name" -> "Diana", "age" -> "33", "CNP" -> "255532142"),
      Map("name" -> "Tatiana", "age" -> "55", "CNP" -> "655532132"),
      Map("name" -> "Rosmaria", "age" -> "12", "CNP" -> "855532172")))), "SORT", "age"))

  def youngAdultHobbiesJ(db: Database): Option[Table] = queryT(PP_SQL_Table_Select(
    queryT(PP_SQL_Table_Filter(
      queryT(PP_SQL_Table_Select_one_table(
        queryDB(PP_SQL_DB_Join(Some(db), "JOIN", "People", "name", "Hobbies", "name")),
        "SELECTTABLE", "joined")), "FILTER",
          Field("age", age => age < "25" && age >= "1")
            && Field("name", name => name.startsWith("J")))),
              "EXTRACT", List("name", "hobby")))

}
