const gdb = db.getSiblingDB("gamesdb");
const configDb = db.getSiblingDB("config");

print("Collections:");
printjson(gdb.getCollectionNames());

print("Document count:");
print(gdb.games_unified_validated.countDocuments());

print("Collection info:");
const collInfo = gdb.getCollectionInfos({ name: "games_unified_validated" });
printjson(collInfo);

print("Shard distribution:");
printjson(gdb.games_unified_validated.getShardDistribution());

if (collInfo.length > 0 && collInfo[0].info && collInfo[0].info.uuid) {
  const collUuid = collInfo[0].info.uuid;
  print("Chunk count by UUID:");
  print(configDb.chunks.countDocuments({ uuid: collUuid }));
} else {
  print("Chunk count by UUID: collection UUID not found");
}

print("sh.status():");
sh.status();