Settings.settings

libraryDependencies := Dependencies.all

SbtAliases.aliases.flatMap { case (alias, command) =>
  addCommandAlias(alias, command)
}

assembly / mainClass := Some("com.sdg.framework.Main")
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}
