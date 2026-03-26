try {
  sh.splitAt("gamesdb.games_unified_validated", { source_platform: "playstation", source_id: MinKey() });
  print("Split at playstation");
} catch (e) {
  print("Split playstation skipped: " + e);
}

try {
  sh.splitAt("gamesdb.games_unified_validated", { source_platform: "steam", source_id: MinKey() });
  print("Split at steam");
} catch (e) {
  print("Split steam skipped: " + e);
}

try {
  sh.moveChunk("gamesdb.games_unified_validated", { source_platform: "playstation", source_id: MinKey() }, "shard2RS");
  print("Moved playstation chunk to shard2RS");
} catch (e) {
  print("Move playstation chunk skipped: " + e);
}

try {
  sh.moveChunk("gamesdb.games_unified_validated", { source_platform: "steam", source_id: MinKey() }, "shard3RS");
  print("Moved steam chunk to shard3RS");
} catch (e) {
  print("Move steam chunk skipped: " + e);
}