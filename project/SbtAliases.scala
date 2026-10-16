object SbtAliases {
  val aliases: Seq[(String, String)] = Seq(
    "t"    -> "test",
    "to"   -> "testOnly",
    "tq"   -> "testQuick",
    "tsf"  -> "testShowFailed",
    "c"    -> "compile",
    "tc"   -> "Test / compile",
    "f"    -> "scalafmt",
    "fc"   -> "scalafmtCheck",
    "tf"   -> "Test / scalafmt",
    "tfc"  -> "Test / scalafmtCheck",
    "prep" -> ";c;tc;fc;tfc"
  )
}
